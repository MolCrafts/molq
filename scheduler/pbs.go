package scheduler

import (
	"context"
	"encoding/json"
	"path"
	"strings"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/transport"
)

var pbsDependencies = map[string]string{"after": "afterany", "after_success": "afterok", "after_failure": "afternotok"}

// PBS maps one allocation onto OpenPBS qsub, qstat and qdel.
type PBS struct{ core }

// NewPBS binds one PBS destination.
func NewPBS(target transport.Transport, cluster map[string]any) *PBS {
	return &PBS{core{transport: target, cluster: cluster}}
}

func (p *PBS) Validate(spec map[string]any) error {
	if err := p.validate(spec); err != nil {
		return err
	}
	if _, ok := mapAny(spec["scheduling"])["comment"]; ok {
		return unsupported("PBS has no exact user comment mapping")
	}
	if p.cluster["pbs_dialect"] != "openpbs" {
		return unsupported("PBS requires explicit pbs_dialect=openpbs for JSON/select semantics")
	}
	resources, scheduling := mapAny(spec["resources"]), mapAny(spec["scheduling"])
	if _, err := memoryMiB(resources); err != nil {
		return err
	}
	nodes, tasks := defaultInt(resources["nodes"], 1), defaultInt(resources["tasks"], 1)
	cpus := defaultInt(resources["cpus_per_task"], 1)
	units, _ := mapAny(spec["execution"])["units"].([]any)
	for _, item := range units {
		launch := mapAny(mapAny(item)["launch"])
		if defaultInt(launch["ranks"], 1) > tasks || defaultInt(launch["threads"], 1) > cpus {
			return protocol.Fail("EXECUTION_RESOURCE_CONFLICT", "MPI launch exceeds allocation layout", nil)
		}
	}
	if tasks%nodes != 0 {
		return unsupported("OpenPBS requires evenly distributed tasks across requested nodes")
	}
	if len(units) > 1 {
		if _, ok := AsInt(resources["tasks"]); !ok {
			return protocol.Fail("RESOURCE_REQUEST_INVALID", "Multi-unit batch execution requires explicit tasks", nil)
		}
	}
	for _, key := range []string{"qos", "reservation", "exclusive"} {
		if _, ok := scheduling[key]; ok {
			return unsupported("These scheduling intents have no configured PBS mapping")
		}
	}
	accelerators := anySlice(resources["accelerators"])
	if len(accelerators) > 1 {
		return unsupported("Multiple accelerator requests have no exact PBS mapping")
	}
	for _, item := range accelerators {
		acc := mapAny(item)
		if acc["kind"] != "gpu" || acc["scope"] != "per_node" || acc["model"] != nil {
			return unsupported("OpenPBS supports only per-node GPU counts in this mapping")
		}
	}
	for _, item := range anySlice(scheduling["dependencies"]) {
		if _, ok := pbsDependencies[stringField(mapAny(item)["condition"])]; !ok {
			return unsupported("Dependency condition has no exact PBS mapping")
		}
	}
	return nil
}

func (p *PBS) Render(spec map[string]any, directory string) (string, error) {
	if err := p.Validate(spec); err != nil {
		return "", err
	}
	resources, scheduling := mapAny(spec["resources"]), mapAny(spec["scheduling"])
	nodes, tasks, cpus := defaultInt(resources["nodes"], 1), defaultInt(resources["tasks"], 1), defaultInt(resources["cpus_per_task"], 1)
	chunk := "select=" + itoa(nodes) + ":ncpus=" + itoa(tasks/nodes*cpus) + ":mpiprocs=" + itoa(tasks/nodes) + ":ompthreads=" + itoa(cpus)
	if memory, err := memoryMiB(resources); err != nil {
		return "", err
	} else if memory != "" {
		chunk += ":mem=" + memory + "mb"
	}
	for _, item := range anySlice(resources["accelerators"]) {
		chunk += ":ngpus=" + itoa(defaultInt(mapAny(item)["quantity"], 0))
	}
	directives := []string{"-l " + chunk, "-o " + quote(path.Join(directory, "stdout")), "-e " + quote(path.Join(directory, "stderr"))}
	if seconds, ok := AsInt(resources["time_limit_seconds"]); ok {
		directives = append(directives, "-l walltime="+itoa(seconds/3600)+":"+two(seconds%3600/60)+":"+two(seconds%60))
	}
	for _, pair := range []struct{ field, flag string }{{"name", "N"}, {"partition", "q"}, {"account", "A"}} {
		if value, ok := scheduling[pair.field].(string); ok && value != "" {
			directives = append(directives, "-"+pair.flag+" "+value)
		}
	}
	if priority, ok := AsInt(scheduling["priority"]); ok {
		directives = append(directives, "-p "+itoa(priority))
	}
	if edges := anySlice(scheduling["dependencies"]); len(edges) > 0 {
		parts := make([]string, len(edges))
		for i, item := range edges {
			edge := mapAny(item)
			parts[i] = pbsDependencies[stringField(edge["condition"])] + ":" + stringField(mapAny(edge["ref"])["native_id"])
		}
		directives = append(directives, "-W depend="+strings.Join(parts, ","))
	}
	body, err := RenderExecution(spec, p.launch)
	if err != nil {
		return "", err
	}
	return "#!/usr/bin/env bash\n" + prefix("#PBS ", directives) + "cd -- " + quote(directory) + " || exit 126\n" + body, nil
}

func (p *PBS) Submit(ctx context.Context, spec map[string]any, directory string) (Job, error) {
	if err := p.prepareRun(ctx, directory, nil); err != nil {
		return Job{}, err
	}
	script, err := p.Render(spec, directory)
	if err != nil {
		return Job{}, err
	}
	text, err := p.nativeWithLabel(ctx, []string{"sh", "-c", stageScript + `qsub "$script"`, "sh", itoa(len(script))}, "qsub", true, []byte(script), true)
	if err != nil {
		return Job{}, err
	}
	identity := strings.TrimSpace(text)
	if identity == "" || len(identity) > 256 || strings.ContainsAny(identity, " \t\r\n") {
		return Job{}, protocol.FailOutcome("OUTCOME_UNKNOWN", "Unparseable accepted PBS identity", "unknown", nil)
	}
	return p.accepted(ctx, p.QueryMany, identity)
}

func (p *PBS) QueryMany(ctx context.Context, ids []string, _ string) (map[string]Job, error) {
	if len(ids) == 0 {
		return map[string]Job{}, nil
	}
	text, err := p.native(ctx, append([]string{"qstat", "-x", "-f", "-F", "json"}, ids...), false, nil, false)
	if err != nil {
		return nil, err
	}
	return parsePBS(text, "accounting")
}

func (p *PBS) List(ctx context.Context, _ string) ([]Job, error) {
	text, err := p.native(ctx, []string{"sh", "-c", `qstat -f -F json -u "$(id -un)"`}, false, nil, false)
	if err != nil {
		return nil, err
	}
	jobs, err := parsePBS(text, "queue")
	if err != nil {
		return nil, err
	}
	out := make([]Job, 0, len(jobs))
	for _, job := range jobs {
		out = append(out, job)
	}
	return out, nil
}

func (p *PBS) History(ctx context.Context, ids []string) (map[string]Job, error) {
	return p.QueryMany(ctx, ids, "")
}

func (p *PBS) HistoryRange(context.Context, time.Time, time.Time) ([]Job, error) {
	return nil, unsupported("This Scheduler does not support historical range queries")
}

func (p *PBS) Cancel(ctx context.Context, job Job) (map[string]any, error) {
	if Terminal(job.State) {
		return map[string]any{"outcome": "already_terminal"}, nil
	}
	_, err := p.native(ctx, []string{"qdel", job.NativeID}, true, nil, false)
	if item, ok := err.(*protocol.Error); ok && item.Kind == "SUBMISSION_REJECTED" {
		return nil, protocol.Fail("CANCEL_REJECTED", "PBS cancellation rejected", item.Context)
	}
	if err != nil {
		return nil, err
	}
	return map[string]any{"outcome": "accepted"}, nil
}

func parsePBS(text, source string) (map[string]Job, error) {
	var data struct {
		Jobs map[string]map[string]any `json:"Jobs"`
	}
	if err := json.Unmarshal([]byte(text), &data); err != nil || data.Jobs == nil {
		return nil, protocol.Fail("SCHEDULER_UNAVAILABLE", "Invalid PBS JSON response", nil)
	}
	jobs := map[string]Job{}
	for identity, row := range data.Jobs {
		raw := stringField(row["job_state"])
		var code *int
		if value, ok := AsInt(row["Exit_status"]); ok {
			code = &value
		}
		state := map[string]string{"Q": "queued", "H": "queued", "W": "queued", "T": "queued", "R": "running", "E": "running", "S": "running", "B": "running"}[raw]
		if state == "" {
			state = "unknown"
		}
		if (raw == "F" || raw == "C") && code != nil {
			switch *code {
			case 0:
				state = "succeeded"
			case 271:
				state = "cancelled"
			default:
				state = "failed"
			}
		}
		output := strings.SplitN(stringField(row["Output_Path"]), ":", 2)
		outputPath := output[len(output)-1]
		errorPath := strings.SplitN(stringField(row["Error_Path"]), ":", 2)
		workdir := ""
		if outputPath != "" {
			workdir = path.Dir(outputPath)
			if path.Base(workdir) == ".molq-control" {
				workdir = path.Dir(workdir)
			}
		}
		ctime := stringField(row["ctime"])
		jobs[identity] = Job{
			NativeID: identity, Incarnation: ctime, State: state, Source: source, RawState: raw,
			ExitCode: code, Workdir: strPtr(workdir), Name: strPtr(stringField(row["Job_Name"])),
			StdoutPath: strPtr(outputPath), StderrPath: strPtr(errorPath[len(errorPath)-1]),
			SubmittedAt: unixStamp(row["ctime"]), StartedAt: unixStamp(row["stime"]),
			EndedAt: endedStamp(row["obittime"], state),
		}
	}
	return jobs, nil
}

func defaultInt(value any, fallback int) int {
	if number, ok := AsInt(value); ok {
		return number
	}
	return fallback
}

func unixStamp(value any) *string {
	number, ok := AsInt(value)
	if !ok || number == 0 {
		return nil
	}
	text := time.Unix(int64(number), 0).UTC().Format(time.RFC3339)
	return &text
}

func endedStamp(value any, state string) *string {
	if !Terminal(state) {
		return nil
	}
	return unixStamp(value)
}
