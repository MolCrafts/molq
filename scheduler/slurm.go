package scheduler

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/transport"
)

var slurmStates = map[string]string{
	"PENDING": "queued", "CONFIGURING": "queued", "RUNNING": "running", "COMPLETING": "running",
	"SUSPENDED": "running", "COMPLETED": "succeeded", "FAILED": "failed", "NODE_FAIL": "failed",
	"OUT_OF_MEMORY": "failed", "BOOT_FAIL": "failed", "CANCELLED": "cancelled", "PREEMPTED": "cancelled",
	"TIMEOUT": "timed_out", "DEADLINE": "timed_out",
}

var slurmDependencies = map[string]string{"after": "afterany", "after_success": "afterok", "after_failure": "afternotok", "after_started": "after"}

// Slurm maps one allocation onto sbatch, squeue, sacct and scancel.
type Slurm struct{ core }

// NewSlurm binds one Slurm destination.
func NewSlurm(target transport.Transport, cluster map[string]any) *Slurm {
	return &Slurm{core{transport: target, cluster: cluster}}
}

func (s *Slurm) launch(unit map[string]any, payload string) (string, error) {
	intent := mapAny(unit["launch"])
	if len(intent) == 0 {
		intent = map[string]any{"kind": "direct"}
	}
	placement := mapAny(unit["placement"])
	if intent["kind"] == "mpi" && s.cluster["launcher"] == "srun" {
		ranks, _ := AsInt(intent["ranks"])
		threads, ok := AsInt(intent["threads"])
		if !ok {
			threads = 1
		}
		return "srun --exclusive --ntasks=" + itoa(ranks) + " --cpus-per-task=" + itoa(threads) + " " + payload, nil
	}
	if intent["kind"] == "direct" && len(placement) > 0 {
		tasks, ok := AsInt(placement["tasks"])
		if !ok {
			tasks = 1
		}
		if tasks != 1 {
			return "", unsupported("Direct placement is exactly one task; use MPI for multiple ranks")
		}
		cpus, ok := AsInt(placement["cpus_per_task"])
		if !ok {
			cpus = 1
		}
		return "srun --exclusive --ntasks=1 --cpus-per-task=" + itoa(cpus) + " " + payload, nil
	}
	return s.core.launch(unit, payload)
}

func (s *Slurm) Validate(spec map[string]any) error {
	if err := s.validateUnits(spec); err != nil {
		return err
	}
	resources := mapAny(spec["resources"])
	if _, err := memoryMiB(resources); err != nil {
		return err
	}
	nodes, ok := AsInt(resources["nodes"])
	if !ok {
		nodes = 1
	}
	units, _ := mapAny(spec["execution"])["units"].([]any)
	if nodes > 1 && len(units) > 1 {
		for _, item := range units {
			unit := mapAny(item)
			launch := mapAny(unit["launch"])
			kind, _ := launch["kind"].(string)
			if kind == "" {
				kind = "direct"
			}
			if kind == "direct" && len(mapAny(unit["placement"])) == 0 {
				return unsupported("Multi-node direct units require explicit native-step placement")
			}
		}
	}
	tasks, hasTasks := AsInt(resources["tasks"])
	if !hasTasks {
		tasks = 1
	}
	if len(units) > 1 && !hasTasks {
		return protocol.Fail("RESOURCE_REQUEST_INVALID", "Multi-unit batch execution requires an explicit task budget", nil)
	}
	cpus, hasCPUs := AsInt(resources["cpus_per_task"])
	if !hasCPUs {
		cpus = 1
	}
	for _, item := range units {
		launch := mapAny(mapAny(item)["launch"])
		ranks, ok := AsInt(launch["ranks"])
		if !ok {
			ranks = 1
		}
		threads, ok := AsInt(launch["threads"])
		if !ok {
			threads = 1
		}
		if ranks > tasks || threads > cpus {
			return protocol.Fail("EXECUTION_RESOURCE_CONFLICT", "MPI launch exceeds the allocation layout", nil)
		}
	}
	accelerators, _ := resources["accelerators"].([]any)
	if len(accelerators) > 1 {
		return unsupported("Multiple accelerator requests require an unambiguous native resource mapping")
	}
	if len(accelerators) == 1 {
		acc := mapAny(accelerators[0])
		if acc["kind"] == "nvidia_mps" && acc["scope"] != "per_node" {
			return unsupported("Slurm MPS is represented per node")
		}
	}
	for _, item := range anySlice(mapAny(spec["scheduling"])["dependencies"]) {
		edge := mapAny(item)
		if _, ok := slurmDependencies[stringField(edge["condition"])]; !ok {
			return unsupported("This dependency condition has no exact Slurm mapping")
		}
	}
	return nil
}

func (s *Slurm) validateUnits(spec map[string]any) error {
	units, _ := mapAny(spec["execution"])["units"].([]any)
	for _, item := range units {
		if _, err := s.launch(mapAny(item), "true"); err != nil {
			return err
		}
	}
	return nil
}

func (s *Slurm) Render(spec map[string]any, directory string) (string, error) {
	if err := s.Validate(spec); err != nil {
		return "", err
	}
	resources, scheduling := mapAny(spec["resources"]), mapAny(spec["scheduling"])
	directives := []string{
		"--chdir=" + quote(directory),
		"--output=" + quote(directory+"/stdout"),
		"--error=" + quote(directory+"/stderr"),
	}
	for _, pair := range []struct{ field, flag string }{{"nodes", "nodes"}, {"tasks", "ntasks"}, {"cpus_per_task", "cpus-per-task"}} {
		if value, ok := AsInt(resources[pair.field]); ok {
			directives = append(directives, "--"+pair.flag+"="+itoa(value))
		}
	}
	if memory, err := memoryMiB(resources); err != nil {
		return "", err
	} else if memory != "" {
		directives = append(directives, "--mem="+memory+"M")
	}
	if seconds, ok := AsInt(resources["time_limit_seconds"]); ok {
		directives = append(directives, "--time="+itoa(seconds/3600)+":"+two(seconds%3600/60)+":"+two(seconds%60))
	}
	for _, pair := range []struct{ field, flag string }{{"name", "job-name"}, {"partition", "partition"}, {"account", "account"}, {"qos", "qos"}, {"reservation", "reservation"}} {
		if value, ok := scheduling[pair.field].(string); ok && value != "" {
			directives = append(directives, "--"+pair.flag+"="+value)
		}
	}
	if priority, ok := AsInt(scheduling["priority"]); ok {
		directives = append(directives, "--priority="+itoa(priority))
	}
	if scheduling["exclusive"] == true {
		directives = append(directives, "--exclusive")
	}
	for _, item := range anySlice(resources["accelerators"]) {
		acc := mapAny(item)
		quantity, _ := AsInt(acc["quantity"])
		if acc["kind"] == "nvidia_mps" {
			directives = append(directives, "--gres=mps:"+itoa(quantity))
			continue
		}
		resource := itoa(quantity)
		if model, ok := acc["model"].(string); ok && model != "" {
			resource = model + ":" + resource
		}
		flag := "gpus"
		if acc["scope"] == "per_node" {
			flag = "gpus-per-node"
		}
		directives = append(directives, "--"+flag+"="+resource)
	}
	if edges := anySlice(scheduling["dependencies"]); len(edges) > 0 {
		parts := make([]string, 0, len(edges))
		for _, item := range edges {
			edge := mapAny(item)
			ref := mapAny(edge["ref"])
			parts = append(parts, slurmDependencies[stringField(edge["condition"])]+":"+stringField(ref["native_id"]))
		}
		directives = append(directives, "--dependency="+strings.Join(parts, ","))
	}
	body, err := RenderExecution(spec, s.launch)
	if err != nil {
		return "", err
	}
	return "#!/usr/bin/env bash\n" + prefix("#SBATCH ", directives) + body, nil
}

func (s *Slurm) Submit(ctx context.Context, spec map[string]any, directory string) (Job, error) {
	script, err := s.stage(ctx, spec, directory, s.Validate, s.Render)
	if err != nil {
		return Job{}, err
	}
	argv := []string{"sbatch", "--parsable"}
	if comment, ok := mapAny(spec["scheduling"])["comment"].(string); ok {
		argv = append(argv, "--comment", comment)
	}
	text, err := s.native(ctx, append(argv, script), true, nil, false)
	if err != nil {
		return Job{}, err
	}
	identity := strings.Split(strings.TrimSpace(text), ";")[0]
	if identity == "" || strings.IndexFunc(identity, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
		return Job{}, protocol.FailOutcome("OUTCOME_UNKNOWN", "Cannot parse accepted allocation identity", "unknown", nil)
	}
	return s.accepted(ctx, s.QueryMany, identity)
}

func (s *Slurm) QueryMany(ctx context.Context, ids []string) (map[string]Job, error) {
	if len(ids) == 0 {
		return map[string]Job{}, nil
	}
	text, err := s.native(ctx, []string{"squeue", "--json", "--jobs", strings.Join(ids, ",")}, false, nil, false)
	if err != nil {
		item, ok := err.(*protocol.Error)
		stderr, _ := item.Context["stderr"].(string)
		code, _ := AsInt(item.Context["returncode"])
		if !ok || code != 1 || !strings.Contains(stderr, "Invalid job id specified") {
			return nil, err
		}
		return s.History(ctx, ids)
	}
	jobs, err := s.parse(text, "queue")
	if err != nil {
		return nil, err
	}
	var missing []string
	for _, id := range ids {
		if _, ok := jobs[id]; !ok {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		history, err := s.History(ctx, missing)
		if err != nil {
			return nil, err
		}
		for id, job := range history {
			jobs[id] = job
		}
	}
	out := map[string]Job{}
	for _, id := range ids {
		if job, ok := jobs[id]; ok {
			out[id] = job
		}
	}
	return out, nil
}

func (s *Slurm) List(ctx context.Context) ([]Job, error) {
	text, err := s.native(ctx, []string{"squeue", "--me", "--json"}, false, nil, false)
	if err != nil {
		return nil, err
	}
	jobs, err := s.parse(text, "queue")
	if err != nil {
		return nil, err
	}
	out := make([]Job, 0, len(jobs))
	for _, job := range jobs {
		out = append(out, job)
	}
	return out, nil
}

func (s *Slurm) History(ctx context.Context, ids []string) (map[string]Job, error) {
	if len(ids) == 0 {
		return map[string]Job{}, nil
	}
	text, err := s.native(ctx, []string{"sacct", "--allocations", "--json", "--duplicates", "--jobs", strings.Join(ids, ",")}, false, nil, false)
	if err != nil {
		return nil, err
	}
	return s.parse(text, "accounting")
}

func (s *Slurm) HistoryRange(ctx context.Context, since, until time.Time) ([]Job, error) {
	if until.Nanosecond() != 0 {
		until = until.Truncate(time.Second).Add(time.Second)
	}
	argv := []string{"sacct", "--json", "--allocations", "--duplicates", "--starttime", since.UTC().Format("2006-01-02T15:04:05"), "--endtime", until.UTC().Format("2006-01-02T15:04:05")}
	command := "exec env TZ=UTC " + joinStrings(argv) + ` --user="$(id -un)"`
	text, err := s.native(ctx, []string{"sh", "-c", command}, false, nil, false)
	if err != nil {
		return nil, err
	}
	return parseSlurmJobs(text, "accounting")
}

func (s *Slurm) Cancel(ctx context.Context, job Job) (map[string]any, error) {
	if Terminal(job.State) {
		return map[string]any{"outcome": "already_terminal"}, nil
	}
	_, err := s.native(ctx, []string{"scancel", job.NativeID}, true, nil, false)
	if err != nil {
		if item, ok := err.(*protocol.Error); ok && item.Kind == "SUBMISSION_REJECTED" {
			return nil, protocol.Fail("CANCEL_REJECTED", "Native cancellation rejected", item.Context)
		}
		return nil, err
	}
	return map[string]any{"outcome": "accepted"}, nil
}

func (s *Slurm) parse(text, source string) (map[string]Job, error) {
	jobs, err := parseSlurmJobs(text, source)
	if err != nil {
		return nil, err
	}
	out := map[string]Job{}
	for _, job := range jobs {
		if previous, ok := out[job.NativeID]; ok && previous.Incarnation != job.Incarnation {
			return nil, protocol.Fail("JOB_IDENTITY_UNVERIFIABLE", "Multiple incarnations match native ID; use historical range query", nil)
		}
		out[job.NativeID] = job
	}
	return out, nil
}

func parseSlurmJobs(text, source string) ([]Job, error) {
	var data struct {
		Errors   any              `json:"errors"`
		Warnings any              `json:"warnings"`
		Jobs     []map[string]any `json:"jobs"`
	}
	if err := json.Unmarshal([]byte(text), &data); err != nil || data.Jobs == nil || nonempty(data.Errors) || slurmWarningsPartial(data.Warnings, source, len(data.Jobs)) {
		return nil, protocol.Fail("SCHEDULER_UNAVAILABLE", "Invalid/partial Slurm JSON response", nil)
	}
	jobs := make([]Job, 0, len(data.Jobs))
	for _, row := range data.Jobs {
		job, err := slurmJob(row, source)
		if err != nil {
			return nil, protocol.Fail("SCHEDULER_UNAVAILABLE", "Invalid/partial Slurm JSON response", nil)
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func slurmWarningsPartial(warnings any, source string, jobs int) bool {
	if !nonempty(warnings) {
		return false
	}
	// Slurm 25.05 squeue emits this warning for a successful empty queue.
	// Restrict the exception to the observed complete response; other warnings
	// can indicate partial results and must not turn missing jobs into absence.
	items, ok := warnings.([]any)
	if !ok || source != "queue" || jobs != 0 || len(items) != 1 {
		return true
	}
	item, ok := items[0].(map[string]any)
	return !ok || len(item) != 2 || item["description"] != "Zero jobs to dump" || item["source"] != ""
}

func slurmJob(row map[string]any, source string) (Job, error) {
	identity := stringField(row["job_id"])
	if identity == "" || strings.IndexFunc(identity, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
		return Job{}, protocol.Fail("SCHEDULER_UNAVAILABLE", "Invalid/partial Slurm JSON response", nil)
	}
	var raw string
	if source == "queue" {
		raw = firstString(row["job_state"])
	} else {
		raw = firstString(mapAny(row["state"])["current"])
	}
	if raw == "" {
		return Job{}, protocol.Fail("SCHEDULER_UNAVAILABLE", "Invalid/partial Slurm JSON response", nil)
	}
	token := strings.TrimRight(strings.Fields(raw)[0], "+")
	state := slurmStates[token]
	if state == "" {
		state = "unknown"
	}
	times := mapAny(row["time"])
	var submitted, started, ended *string
	if source == "queue" {
		submitted = slurmTime(row["submit_time"])
		started = slurmTime(row["start_time"])
		ended = slurmTime(row["end_time"])
	} else {
		submitted = slurmTime(times["submission"])
		started = slurmTime(times["start"])
		ended = slurmTime(times["end"])
	}
	exitCode := slurmExitCode(row)
	if token == "COMPLETING" {
		if ended != nil {
			if exitCode == nil || *exitCode == 0 {
				state = "succeeded"
			} else {
				state = "failed"
			}
		} else {
			state = "unknown"
		}
	}
	if !Terminal(state) {
		ended = nil
		exitCode = nil
	}
	incarnation := ""
	if submitted != nil {
		incarnation = *submitted
	}
	workdir := stringField(row["working_directory"])
	if source == "queue" {
		workdir = stringField(row["current_working_directory"])
	}
	comment := row["comment"]
	if object, ok := comment.(map[string]any); ok {
		comment = object["job"]
	}
	var commentPtr *string
	if text, ok := comment.(string); ok {
		commentPtr = &text
	} else if comment != nil {
		return Job{}, protocol.Fail("SCHEDULER_UNAVAILABLE", "Invalid/partial Slurm JSON response", nil)
	}
	return Job{
		NativeID: identity, Incarnation: incarnation, State: state, Source: source, RawState: raw,
		ExitCode: exitCode, Workdir: strPtr(workdir), Name: strPtr(stringField(row["name"])),
		StdoutPath: strPtr(stringField(row["standard_output"])), StderrPath: strPtr(stringField(row["standard_error"])),
		Comment: commentPtr, SubmittedAt: submitted, StartedAt: started, EndedAt: ended,
	}, nil
}

func slurmExitCode(row map[string]any) *int {
	exitCode, ok := row["exit_code"].(map[string]any)
	if !ok {
		return nil
	}
	status := slurmNumber(exitCode["return_code"])
	signal := exitCode["signal"]
	var signalID *int
	if object, ok := signal.(map[string]any); ok {
		signalID = slurmNumber(object["id"])
	} else {
		signalID = slurmNumber(signal)
	}
	if status != nil && *status != 0 {
		return status
	}
	if signalID != nil && *signalID != 0 {
		code := 128 + *signalID
		return &code
	}
	return status
}

func slurmNumber(value any) *int {
	if object, ok := value.(map[string]any); ok {
		if object["set"] == false || object["infinite"] == true {
			return nil
		}
		value = object["number"]
	}
	number, ok := AsInt(value)
	if !ok {
		return nil
	}
	return &number
}

func slurmTime(value any) *string {
	number := slurmNumber(value)
	if number == nil || *number <= 0 {
		return nil
	}
	text := time.Unix(int64(*number), 0).UTC().Format(time.RFC3339)
	return &text
}

func prefix(flag string, lines []string) string {
	var out strings.Builder
	for _, line := range lines {
		out.WriteString(flag)
		out.WriteString(line)
		out.WriteByte('\n')
	}
	return out.String()
}

func two(value int) string {
	if value < 10 {
		return "0" + itoa(value)
	}
	return itoa(value)
}

func firstString(value any) string {
	switch item := value.(type) {
	case string:
		return item
	case []any:
		if len(item) == 0 {
			return ""
		}
		return stringField(item[0])
	default:
		return stringField(value)
	}
}

func anySlice(value any) []any {
	items, _ := value.([]any)
	return items
}

func stringField(value any) string {
	switch item := value.(type) {
	case string:
		return item
	case json.Number:
		return item.String()
	case float64:
		return itoa(int(item))
	case int:
		return itoa(item)
	case int64:
		return itoa(int(item))
	default:
		return ""
	}
}

func joinStrings(argv []string) string {
	parts := make([]string, len(argv))
	for i, arg := range argv {
		parts[i] = quote(arg)
	}
	return strings.Join(parts, " ")
}
