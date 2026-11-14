package serve

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/scheduler"
)

func TestRequestKeyRepeatsTheSameSubmission(t *testing.T) {
	root := t.TempDir()
	service, err := Open(filepath.Join(root, "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	registered, err := service.Call("clusters.register", map[string]any{
		"name": "local", "scheduler": "shell",
	})
	if err != nil {
		t.Fatal(err)
	}
	params := map[string]any{
		"cluster":     registered.(map[string]any)["id"],
		"request_key": "once",
		"spec": map[string]any{"execution": map[string]any{"units": []any{
			map[string]any{"id": "main", "command": map[string]any{"kind": "argv", "argv": []any{"echo", "once"}}},
		}}},
	}
	first, err := service.Call("jobs.submit", params)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Call("jobs.submit", params)
	if err != nil {
		t.Fatal(err)
	}
	if first.(map[string]any)["ref"].(map[string]any)["native_id"] != second.(map[string]any)["ref"].(map[string]any)["native_id"] {
		t.Fatalf("repeated key created another job: %#v %#v", first, second)
	}
	params["spec"].(map[string]any)["execution"].(map[string]any)["units"].([]any)[0].(map[string]any)["command"].(map[string]any)["argv"] = []any{"echo", "other"}
	if _, err := service.Call("jobs.submit", params); err == nil {
		t.Fatal("expected conflict")
	}
}

func TestRequestKeyWaitsForTheInflightCall(t *testing.T) {
	root := t.TempDir()
	service, err := Open(filepath.Join(root, "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	registered, err := service.Call("clusters.register", map[string]any{
		"name": "local", "scheduler": "shell",
	})
	if err != nil {
		t.Fatal(err)
	}
	params := map[string]any{
		"cluster": registered.(map[string]any)["id"], "request_key": "parallel",
		"spec": map[string]any{"execution": map[string]any{"units": []any{
			map[string]any{"id": "main", "command": map[string]any{"kind": "argv", "argv": []any{"echo", "parallel"}}},
		}}},
	}
	var group sync.WaitGroup
	results := make([]any, 2)
	for i := range results {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			value, err := service.Call("jobs.submit", params)
			if err != nil {
				t.Error(err)
				return
			}
			results[i] = value
		}(i)
	}
	group.Wait()
	if results[0] == nil || results[1] == nil {
		t.Fatal(results)
	}
	left := results[0].(map[string]any)["ref"].(map[string]any)["native_id"]
	right := results[1].(map[string]any)["ref"].(map[string]any)["native_id"]
	if left != right {
		t.Fatalf("ids = %v %v", left, right)
	}
}

func TestSubmissionRecoversAfterLostNativeReply(t *testing.T) {
	service, root := purgeService(t)
	id := register(t, service, map[string]any{"name": "local", "scheduler": "slurm"})
	directory := filepath.Join(root, "registry.db")
	native := &lostSubmissionScheduler{}
	service.schedulers[id] = native
	params := map[string]any{
		"cluster": id, "request_key": "lost-reply",
		"spec": map[string]any{"execution": map[string]any{"units": []any{
			map[string]any{"id": "main", "command": map[string]any{"kind": "argv", "argv": []any{"true"}}},
		}}},
	}
	if _, err := service.Call("jobs.submit", params); err == nil {
		t.Fatal("expected unknown result")
	}
	second, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	second.ensure()
	second.schedulers[id] = native
	var group sync.WaitGroup
	for range 4 {
		group.Go(func() {
			got, err := second.Call("jobs.submit", params)
			if err != nil || mapValue(mapValue(got)["ref"])["native_id"] != "42" {
				t.Errorf("replayed submission: %v, %v", got, err)
			}
		})
	}
	group.Wait()
	if native.submits != 1 || native.recoveries != 1 {
		t.Fatalf("native calls: %d submit, %d recover", native.submits, native.recoveries)
	}
}

type lostSubmissionScheduler struct {
	scheduler.Scheduler
	directory  string
	submits    int
	recoveries int
}

func (*lostSubmissionScheduler) Validate(map[string]any) error { return nil }
func (s *lostSubmissionScheduler) Submit(_ context.Context, _ map[string]any, directory string) (scheduler.Job, error) {
	s.directory = directory
	s.submits++
	return scheduler.Job{}, protocol.FailOutcome("OUTCOME_UNKNOWN", "lost reply", "unknown", nil)
}
func (s *lostSubmissionScheduler) RecoverSubmission(_ context.Context, lookup scheduler.SubmissionLookup) (scheduler.Job, error) {
	s.recoveries++
	if lookup.Directory != s.directory {
		return scheduler.Job{}, protocol.FailOutcome("OUTCOME_UNKNOWN", "missing match", "unknown", nil)
	}
	return scheduler.Job{NativeID: "42", Incarnation: "fixture", State: "queued", Source: "queue"}, nil
}
