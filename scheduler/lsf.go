package scheduler

import (
	"context"
	"encoding/json"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/transport"
)

var lsfID = regexp.MustCompile(`Job <(\d+)>`)

var lsfDependencies = map[string]string{"after": "ended", "after_success": "done", "after_failure": "exit", "after_started": "started"}

// LSF maps one allocation onto bsub, bjobs and bkill.
type LSF struct{ core }

// NewLSF binds one LSF destination.
func NewLSF(target transport.Transport, cluster map[string]any) *LSF {
	return &LSF{core{transport: target, cluster: cluster}}
}

func (l *LSF) Validate(spec map[string]any) error {
	if err := l.validate(spec); err != nil {
		return err
	}
	resources, scheduling := mapAny(spec["resources"]), mapAny(spec["scheduling"])
	tasks := defaultInt(resources["tasks"], 1)
	for _, item := range anySlice(mapAny(spec["execution"])["units"]) {
		launch := mapAny(mapAny(item)["launch"])
		if defaultInt(launch["ranks"], 1) > tasks || defaultInt(launch["threads"], 1) != 1 {
			return protocol.Fail("EXECUTION_RESOURCE_CONFLICT", "MPI launch exceeds the LSF allocation layout", nil)
		}
	}
	if defaultInt(resources["nodes"], 1) != 1 || defaultInt(resources["cpus_per_task"], 1) != 1 {
		return unsupported("LSF mapping currently requires a single-node single-thread task layout")
	}
	if _, ok := resources["memory_bytes"]; ok || len(anySlice(resources["accelerators"])) > 0 {
		return unsupported("LSF memory/GPU/MPS allocation semantics require an explicit site mapping")
	}
	for _, key := range []string{"qos", "reservation"} {
		if _, ok := scheduling[key]; ok {
			return unsupported("No exact LSF mapping for these scheduling intents")
		}
	}
	if len(anySlice(mapAny(spec["execution"])["units"])) > 1 {
		if _, ok := AsInt(resources["tasks"]); !ok {
			return protocol.Fail("RESOURCE_REQUEST_INVALID", "Multi-unit batch execution requires explicit tasks", nil)
		}
	}
	return nil
}

func (l *LSF) Render(spec map[string]any, directory string) (string, error) {
	if err := l.Validate(spec); err != nil {
		return "", err
	}
	resources, scheduling := mapAny(spec["resources"]), mapAny(spec["scheduling"])
	directives := []string{
		"-cwd " + quote(directory), "-o " + quote(path.Join(directory, "stdout")), "-e " + quote(path.Join(directory, "stderr")),
		"-n " + itoa(defaultInt(resources["tasks"], 1)), `-R "span[hosts=1]"`,
	}
	if seconds, ok := AsInt(resources["time_limit_seconds"]); ok {
		minutes := (seconds + 59) / 60
		directives = append(directives, "-W "+itoa(minutes/60)+":"+two(minutes%60))
	}
	for _, pair := range []struct{ field, flag string }{{"name", "J"}, {"partition", "q"}, {"account", "P"}} {
		if value, ok := scheduling[pair.field].(string); ok && value != "" {
			directives = append(directives, "-"+pair.flag+" "+value)
		}
	}
	if priority, ok := AsInt(scheduling["priority"]); ok {
		directives = append(directives, "-sp "+itoa(priority))
	}
	if scheduling["exclusive"] == true {
		directives = append(directives, "-x")
	}
	if edges := anySlice(scheduling["dependencies"]); len(edges) > 0 {
		parts := make([]string, len(edges))
		for i, item := range edges {
			edge := mapAny(item)
			condition := lsfDependencies[stringField(edge["condition"])]
			parts[i] = condition + "(" + stringField(mapAny(edge["ref"])["native_id"]) + ")"
		}
		directives = append(directives, "-w "+quote(strings.Join(parts, " && ")))
	}
	body, err := RenderExecution(spec, l.launch)
	if err != nil {
		return "", err
	}
	return "#!/usr/bin/env bash\n" + prefix("#BSUB ", directives) + body, nil
}

func (l *LSF) Submit(ctx context.Context, spec map[string]any, directory string) (Job, error) {
	if err := l.prepareRun(ctx, directory, nil); err != nil {
		return Job{}, err
	}
	script, err := l.Render(spec, directory)
	if err != nil {
		return Job{}, err
	}
	argv := []string{"bsub"}
	if comment, ok := mapAny(spec["scheduling"])["comment"].(string); ok {
		argv = append(argv, "-Jd", comment)
	}
	text, err := l.native(ctx, argv, true, []byte(script), true)
	if err != nil {
		return Job{}, err
	}
	match := lsfID.FindStringSubmatch(text)
	if match == nil {
		return Job{}, protocol.FailOutcome("OUTCOME_UNKNOWN", "Unparseable accepted LSF identity", "unknown", nil)
	}
	return l.accepted(ctx, l.QueryMany, match[1])
}

func (l *LSF) QueryMany(ctx context.Context, ids []string, _ string) (map[string]Job, error) {
	if len(ids) == 0 {
		return map[string]Job{}, nil
	}
	argv := []string{"bjobs", "-a", "-json", "-o", "jobid stat submit_time exec_cwd output_file error_file job_name job_description exit_code"}
	text, err := l.native(ctx, append(argv, ids...), false, nil, false)
	if err != nil {
		return nil, err
	}
	return parseLSF(text)
}

func (l *LSF) List(ctx context.Context, _ string) ([]Job, error) {
	text, err := l.native(ctx, []string{"bjobs", "-json", "-o", "jobid stat submit_time exec_cwd output_file error_file job_name job_description exit_code"}, false, nil, false)
	if err != nil {
		return nil, err
	}
	jobs, err := parseLSF(text)
	if err != nil {
		return nil, err
	}
	out := make([]Job, 0, len(jobs))
	for _, job := range jobs {
		out = append(out, job)
	}
	return out, nil
}

func (l *LSF) History(ctx context.Context, ids []string) (map[string]Job, error) {
	return l.QueryMany(ctx, ids, "")
}

func (l *LSF) HistoryRange(context.Context, time.Time, time.Time) ([]Job, error) {
	return nil, unsupported("This Scheduler does not support historical range queries")
}

func (l *LSF) Cancel(ctx context.Context, job Job) (map[string]any, error) {
	if Terminal(job.State) {
		return map[string]any{"outcome": "already_terminal"}, nil
	}
	_, err := l.native(ctx, []string{"bkill", job.NativeID}, true, nil, false)
	if item, ok := err.(*protocol.Error); ok && item.Kind == "SUBMISSION_REJECTED" {
		return nil, protocol.Fail("CANCEL_REJECTED", "LSF cancellation rejected", item.Context)
	}
	if err != nil {
		return nil, err
	}
	return map[string]any{"outcome": "accepted"}, nil
}

func parseLSF(text string) (map[string]Job, error) {
	var data struct {
		Records []map[string]any `json:"RECORDS"`
	}
	if err := json.Unmarshal([]byte(text), &data); err != nil {
		return nil, protocol.Fail("SCHEDULER_UNAVAILABLE", "Invalid/partial LSF query response", nil)
	}
	jobs := map[string]Job{}
	for _, row := range data.Records {
		if row["ERROR"] != nil {
			return nil, protocol.Fail("SCHEDULER_UNAVAILABLE", "Invalid/partial LSF query response", nil)
		}
		identity := stringField(row["JOBID"])
		raw := stringField(row["STAT"])
		state := map[string]string{"PEND": "queued", "WAIT": "queued", "RUN": "running", "PSUSP": "running", "USUSP": "running", "SSUSP": "running", "DONE": "succeeded", "EXIT": "failed", "ZOMBI": "unknown", "UNKWN": "unknown"}[raw]
		if state == "" {
			state = "unknown"
		}
		var code *int
		if text, ok := row["EXIT_CODE"].(string); ok {
			if number, err := strconv.Atoi(text); err == nil {
				code = &number
			}
		} else if number, ok := AsInt(row["EXIT_CODE"]); ok {
			code = &number
		}
		output := stringField(row["OUTPUT_FILE"])
		errFile := stringField(row["ERROR_FILE"])
		workdir := stringField(row["EXEC_CWD"])
		if workdir == "" || workdir == "-" {
			if strings.HasPrefix(output, "/") {
				workdir = path.Dir(output)
				if path.Base(workdir) == ".molq-control" {
					workdir = path.Dir(workdir)
				}
			} else {
				workdir = ""
			}
		}
		submitted := stringField(row["SUBMIT_TIME"])
		incarnation := submitted
		if submitted != "" && output != "" && output != "-" {
			incarnation = submitted + "|" + output
		}
		jobs[identity] = Job{
			NativeID: identity, Incarnation: incarnation, State: state, Source: "accounting", RawState: raw,
			ExitCode: code, Workdir: strPtr(workdir), Name: strPtr(stringField(row["JOB_NAME"])),
			StdoutPath: blankNil(output), StderrPath: blankNil(errFile),
			Comment: blankNil(stringField(row["JOB_DESCRIPTION"])),
		}
	}
	return jobs, nil
}

func blankNil(value string) *string {
	if value == "" || value == "-" {
		return nil
	}
	return &value
}
