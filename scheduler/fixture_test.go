package scheduler

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/transport"
)

type fakeTransport struct {
	calls    [][]string
	files    map[string][]byte
	scripted []transport.Result
}

func script(values ...any) *fakeTransport {
	fake := &fakeTransport{files: map[string][]byte{}}
	for _, value := range values {
		switch item := value.(type) {
		case string:
			fake.scripted = append(fake.scripted, transport.Result{Stdout: []byte(item)})
		case transport.Result:
			fake.scripted = append(fake.scripted, item)
		}
	}
	return fake
}

func (f *fakeTransport) Run(_ context.Context, argv []string, _ time.Duration, _ []byte, _ bool) (transport.Result, error) {
	f.calls = append(f.calls, append([]string{}, argv...))
	if len(f.scripted) == 0 {
		return transport.Result{}, nil
	}
	result := f.scripted[0]
	f.scripted = f.scripted[1:]
	return result, nil
}

func (f *fakeTransport) Read(context.Context, string, int, int) ([]byte, error) {
	return nil, nil
}

func (f *fakeTransport) Write(_ context.Context, path string, data []byte, _ *int) error {
	f.files[path] = append([]byte{}, data...)
	return nil
}

func specWith(resources, scheduling map[string]any, launch map[string]any) map[string]any {
	unit := map[string]any{"id": "main", "command": map[string]any{"kind": "argv", "argv": []any{"echo", "ok"}}}
	if launch != nil {
		unit["launch"] = launch
	}
	spec := map[string]any{"execution": map[string]any{
		"units": []any{unit},
		"plan":  map[string]any{"kind": "unit", "ref": "main"},
	}}
	if resources != nil {
		spec["resources"] = resources
	}
	if scheduling != nil {
		spec["scheduling"] = scheduling
	}
	return spec
}

func TestCancelAckIsNotTerminal(t *testing.T) {
	for _, makeScheduler := range []func(transport.Transport) Scheduler{
		func(target transport.Transport) Scheduler { return NewSlurm(target, map[string]any{}) },
		func(target transport.Transport) Scheduler {
			return NewPBS(target, map[string]any{"pbs_dialect": "openpbs"})
		},
		func(target transport.Transport) Scheduler { return NewLSF(target, map[string]any{}) },
	} {
		fake := script()
		result, err := makeScheduler(fake).Cancel(context.Background(), Job{NativeID: "123", Incarnation: "timestamp", State: "running", Source: "queue"})
		if err != nil {
			t.Fatal(err)
		}
		if result["outcome"] != "accepted" || len(fake.calls) != 1 {
			t.Fatalf("result=%v calls=%v", result, fake.calls)
		}
	}
}

func TestCancelFailureIsExplicit(t *testing.T) {
	for _, makeScheduler := range []func(transport.Transport) Scheduler{
		func(target transport.Transport) Scheduler { return NewSlurm(target, map[string]any{}) },
		func(target transport.Transport) Scheduler { return NewPBS(target, map[string]any{}) },
		func(target transport.Transport) Scheduler { return NewLSF(target, map[string]any{}) },
	} {
		_, err := makeScheduler(script(transport.Result{ReturnCode: 1, Stderr: []byte("denied")})).Cancel(context.Background(), Job{NativeID: "123", Incarnation: "timestamp", State: "running"})
		item, _ := err.(*protocol.Error)
		if item == nil || item.Kind != "CANCEL_REJECTED" {
			t.Fatalf("err=%v", err)
		}
	}
}

func TestCancelNetworkFailureIsUnknown(t *testing.T) {
	_, err := NewSlurm(script(transport.Result{ReturnCode: 255, Stderr: []byte("SSH failed")}), map[string]any{}).Cancel(context.Background(), Job{NativeID: "123", Incarnation: "t", State: "running"})
	item, _ := err.(*protocol.Error)
	if item == nil || item.Kind != "OUTCOME_UNKNOWN" || item.Outcome != "unknown" {
		t.Fatalf("err=%#v", err)
	}
}

func TestQueryErrorIsNotEmpty(t *testing.T) {
	_, err := NewPBS(script(transport.Result{ReturnCode: 1, Stderr: []byte("unavailable")}), map[string]any{}).QueryMany(context.Background(), []string{"123"})
	item, _ := err.(*protocol.Error)
	if item == nil || item.Kind != "SCHEDULER_UNAVAILABLE" {
		t.Fatalf("err=%v", err)
	}
}

func TestSlurmRenderResources(t *testing.T) {
	spec := specWith(map[string]any{
		"nodes": 2, "tasks": 4, "cpus_per_task": 2, "memory_bytes": "1048577", "time_limit_seconds": 61,
		"accelerators": []any{map[string]any{"kind": "nvidia_mps", "quantity": 20, "scope": "per_node"}},
	}, map[string]any{
		"partition": "gpu", "account": "science", "priority": 0,
		"dependencies": []any{map[string]any{"condition": "after_success", "ref": map[string]any{"native_id": "42"}}},
	}, map[string]any{"kind": "mpi", "ranks": 4, "threads": 2})
	scriptText, err := NewSlurm(script(), map[string]any{"launcher": "srun"}).Render(spec, "/target/a b")
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"--nodes=2", "--ntasks=4", "--cpus-per-task=2", "--mem=2M", "--time=0:01:01", "--gres=mps:20", "--dependency=afterok:42", "--priority=0", "srun --exclusive --ntasks=4", "export OMP_NUM_THREADS=2"} {
		if !strings.Contains(scriptText, expected) {
			t.Fatalf("missing %s\n%s", expected, scriptText)
		}
	}
	if strings.Contains(scriptText, "nvidia-cuda-mps-control") {
		t.Fatal(scriptText)
	}
}

func TestSlurmAfterStarted(t *testing.T) {
	spec := specWith(nil, map[string]any{
		"dependencies": []any{map[string]any{"condition": "after_started", "ref": map[string]any{"native_id": "42"}}},
	}, nil)
	scriptText, err := NewSlurm(script(), map[string]any{}).Render(spec, "/target")
	if err != nil || !strings.Contains(scriptText, "--dependency=after:42") {
		t.Fatalf("err=%v script=%s", err, scriptText)
	}
}

func TestSlurmGPUModel(t *testing.T) {
	spec := specWith(map[string]any{"accelerators": []any{map[string]any{"kind": "gpu", "quantity": 2, "scope": "total", "model": "a100"}}}, nil, nil)
	scriptText, err := NewSlurm(script(), map[string]any{}).Render(spec, "/target")
	if err != nil || !strings.Contains(scriptText, "--gpus=a100:2") {
		t.Fatalf("err=%v script=%s", err, scriptText)
	}
}

func TestSlurmMPSScopeRejected(t *testing.T) {
	spec := specWith(map[string]any{"accelerators": []any{map[string]any{"kind": "nvidia_mps", "quantity": 20, "scope": "total"}}}, nil, nil)
	err := NewSlurm(script(), map[string]any{}).Validate(spec)
	item, _ := err.(*protocol.Error)
	if item == nil || item.Kind != "SCHEDULER_UNSUPPORTED" {
		t.Fatal(err)
	}
}

func TestSlurmQueueMissUsesAccounting(t *testing.T) {
	fake := script(
		transport.Result{ReturnCode: 1, Stderr: []byte("Invalid job id specified")},
		`{"jobs":[{"job_id":"42","state":{"current":"COMPLETED"},"time":{"submission":1710000000,"end":1710000010},"exit_code":{"return_code":0,"signal":{"id":0}},"name":"name"}]}`,
	)
	jobs, err := NewSlurm(fake, map[string]any{}).QueryMany(context.Background(), []string{"42"})
	if err != nil {
		t.Fatal(err)
	}
	if jobs["42"].State != "succeeded" || fake.calls[1][0] != "sacct" {
		t.Fatalf("job=%+v calls=%v", jobs["42"], fake.calls)
	}
}

func TestSlurmSubmitUsesRenderer(t *testing.T) {
	fake := script("123;cluster", `{"jobs":[{"job_id":"123","job_state":"PENDING","submit_time":1710000000,"current_working_directory":"/target/job","name":"test"}]}`)
	job, err := NewSlurm(fake, map[string]any{}).Submit(context.Background(), specWith(nil, nil, nil), "/target/job")
	if err != nil {
		t.Fatal(err)
	}
	if job.NativeID != "123" || !strings.Contains(string(fake.files["/target/job/job.sh"]), "#SBATCH") {
		t.Fatalf("job=%+v file=%s", job, fake.files["/target/job/job.sh"])
	}
	if strings.Join(fake.calls[0], " ") != "sbatch --parsable /target/job/job.sh" {
		t.Fatal(fake.calls[0])
	}
}

func TestSlurmCompletingWithEndUsesExitCode(t *testing.T) {
	done := `{"jobs":[{"job_id":"3711469","state":{"current":"COMPLETING"},"time":{"submission":1710000000,"end":1710000010},"exit_code":{"return_code":0,"signal":{"id":0}},"name":"atv-cuda-nddo"}]}`
	jobs, err := parseSlurmJobs(done, "accounting")
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].State != "succeeded" || jobs[0].RawState != "COMPLETING" || jobs[0].EndedAt == nil || jobs[0].ExitCode == nil || *jobs[0].ExitCode != 0 {
		t.Fatalf("%+v", jobs[0])
	}
	failed := `{"jobs":[{"job_id":"3711469","state":{"current":"COMPLETING"},"time":{"submission":1710000000,"end":1710000010},"exit_code":{"return_code":8},"name":"atv-cuda-nddo"}]}`
	jobs, err = parseSlurmJobs(failed, "accounting")
	if err != nil || jobs[0].State != "failed" || jobs[0].ExitCode == nil || *jobs[0].ExitCode != 8 {
		t.Fatalf("err=%v job=%+v", err, jobs[0])
	}
}

func TestSlurmCompletingWithoutEndStaysOpen(t *testing.T) {
	body := `{"jobs":[{"job_id":"7","state":{"current":"COMPLETING"},"time":{"submission":1710000000},"name":"open"}]}`
	jobs, err := parseSlurmJobs(body, "accounting")
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].State != "unknown" || jobs[0].EndedAt != nil || Terminal(jobs[0].State) {
		t.Fatalf("%+v", jobs[0])
	}
}

func TestMalformedSlurmJSON(t *testing.T) {
	if _, err := parseSlurmJobs("wrong", "accounting"); err == nil {
		t.Fatal("expected error")
	}
}

func TestPBSRenderAndFinishedTruth(t *testing.T) {
	spec := specWith(map[string]any{
		"nodes": 2, "tasks": 4, "cpus_per_task": 2, "memory_bytes": "1048577", "time_limit_seconds": 120,
		"accelerators": []any{map[string]any{"kind": "gpu", "quantity": 1, "scope": "per_node"}},
	}, map[string]any{
		"name": "hello", "partition": "normal", "account": "science", "priority": 10,
		"dependencies": []any{map[string]any{"condition": "after", "ref": map[string]any{"native_id": "7.host"}}},
	}, nil)
	scriptText, err := NewPBS(script(), map[string]any{"pbs_dialect": "openpbs"}).Render(spec, "/target/job")
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"select=2:ncpus=4:mpiprocs=2:ompthreads=2:mem=2mb:ngpus=1", "depend=afterany:7.host"} {
		if !strings.Contains(scriptText, expected) {
			t.Fatalf("missing %s\n%s", expected, scriptText)
		}
	}
	fake := script(`{"Jobs":{"123.host":{"job_state":"F","ctime":123456,"Exit_status":0,"Output_Path":"host:/target/job/stdout"}}}`)
	jobs, err := NewPBS(fake, map[string]any{"pbs_dialect": "openpbs"}).QueryMany(context.Background(), []string{"123.host"})
	if err != nil {
		t.Fatal(err)
	}
	if jobs["123.host"].State != "succeeded" || jobs["123.host"].Workdir == nil || *jobs["123.host"].Workdir != "/target/job" {
		t.Fatalf("%+v", jobs["123.host"])
	}
}

func TestNoMPSEmulation(t *testing.T) {
	spec := specWith(map[string]any{"accelerators": []any{map[string]any{"kind": "nvidia_mps", "quantity": 20, "scope": "per_node"}}}, nil, nil)
	for _, target := range []Scheduler{NewPBS(script(), map[string]any{"pbs_dialect": "openpbs"}), NewLSF(script(), map[string]any{})} {
		_, err := target.Render(spec, "/target")
		item, _ := err.(*protocol.Error)
		if item == nil || item.Kind != "SCHEDULER_UNSUPPORTED" {
			t.Fatal(err)
		}
	}
}

func TestLSFDirectivesAndState(t *testing.T) {
	spec := specWith(map[string]any{"tasks": 2, "time_limit_seconds": 61}, map[string]any{
		"name": "one", "partition": "normal", "account": "project", "priority": 4, "exclusive": true,
		"dependencies": []any{map[string]any{"condition": "after_failure", "ref": map[string]any{"native_id": "10"}}},
	}, nil)
	scriptText, err := NewLSF(script(), map[string]any{}).Render(spec, "/target/job")
	if err != nil || !strings.Contains(scriptText, "-W 0:02") || !strings.Contains(scriptText, "-w 'exit(10)'") {
		t.Fatalf("err=%v script=%s", err, scriptText)
	}
	jobs, err := NewLSF(script(`{"RECORDS":[{"JOBID":"10","STAT":"DONE","SUBMIT_TIME":"2026-10-07","EXEC_CWD":"/target/job","EXIT_CODE":"0"}]}`), map[string]any{}).QueryMany(context.Background(), []string{"10"})
	if err != nil {
		t.Fatal(err)
	}
	if jobs["10"].State != "succeeded" || jobs["10"].ExitCode == nil || *jobs["10"].ExitCode != 0 {
		t.Fatalf("%+v", jobs["10"])
	}
}

func TestShellRejectsResourcesAndUnboundMPI(t *testing.T) {
	if err := NewShell(script(), map[string]any{}).Validate(specWith(map[string]any{"tasks": 1}, nil, nil)); err == nil {
		t.Fatal("expected resource rejection")
	}
	err := NewShell(script(), map[string]any{}).Validate(specWith(nil, nil, map[string]any{"kind": "mpi", "ranks": 2}))
	item, _ := err.(*protocol.Error)
	if item == nil || item.Kind != "SCHEDULER_UNSUPPORTED" {
		t.Fatal(err)
	}
}

func TestPostAcceptanceQueryFailureRemainsUnknown(t *testing.T) {
	_, err := NewSlurm(script("123", transport.Result{ReturnCode: 1, Stderr: []byte("accounting unavailable")}), map[string]any{}).Submit(context.Background(), specWith(nil, nil, nil), "/target")
	item, _ := err.(*protocol.Error)
	if item == nil || item.Kind != "OUTCOME_UNKNOWN" || item.Context["native_id"] == nil {
		t.Fatalf("%#v", err)
	}
}

func TestLSFUsesNativeOutputPaths(t *testing.T) {
	fake := script(`{"RECORDS":[{"JOBID":"42","STAT":"PEND","SUBMIT_TIME":"Oct 7 10:00","EXEC_CWD":"-","OUTPUT_FILE":"/target/allocation/stdout","ERROR_FILE":"/target/allocation/stderr"}]}`)
	jobs, err := NewLSF(fake, map[string]any{}).QueryMany(context.Background(), []string{"42"})
	if err != nil {
		t.Fatal(err)
	}
	job := jobs["42"]
	if job.Workdir == nil || *job.Workdir != "/target/allocation" || job.StdoutPath == nil || *job.StdoutPath != "/target/allocation/stdout" {
		t.Fatalf("%+v", job)
	}
	if job.Incarnation != "Oct 7 10:00|/target/allocation/stdout" {
		t.Fatal(job.Incarnation)
	}
}
