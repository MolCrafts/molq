package serve

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/scheduler"
)

type blockingScheduler struct {
	scheduler.Scheduler
	job     scheduler.Job
	started chan<- string
	proceed <-chan struct{}
}

func (s *blockingScheduler) QueryMany(ctx context.Context, ids []string, _ string) (map[string]scheduler.Job, error) {
	s.started <- s.job.NativeID
	select {
	case <-s.proceed:
		return map[string]scheduler.Job{s.job.NativeID: s.job}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *blockingScheduler) Cancel(context.Context, scheduler.Job) (map[string]any, error) {
	return map[string]any{"outcome": "accepted"}, nil
}

func batchService(t *testing.T, count int, started chan<- string, proceed <-chan struct{}) (*Service, []map[string]any) {
	t.Helper()
	root := t.TempDir()
	service, err := Open(filepath.Join(root, "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	var clusters []map[string]any
	for i := range count {
		registered, err := service.Call("clusters.register", map[string]any{
			"name": fmt.Sprintf("cluster-%d", i), "scheduler": "shell",
		})
		if err != nil {
			t.Fatal(err)
		}
		clusters = append(clusters, registered.(map[string]any))
	}
	var refs []map[string]any
	for i, cluster := range clusters {
		item := &blockingScheduler{job: scheduler.Job{
			NativeID: fmt.Sprintf("job-%d", i), Incarnation: "submitted-1", State: "running", Source: "queue",
		}, started: started, proceed: proceed}
		service.schedulers[cluster["id"].(string)] = item
		ref, err := service.ref(cluster, item.job)
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	return service, refs
}

func TestClusterBatchesRunConcurrentlyAndPreserveOrder(t *testing.T) {
	for _, method := range []string{"jobs.get_many", "jobs.cancel_many"} {
		t.Run(method, func(t *testing.T) {
			started := make(chan string, 2)
			proceed := make(chan struct{})
			service, refs := batchService(t, 2, started, proceed)
			defer func() {
				if proceed != nil {
					close(proceed)
				}
			}()
			params := map[string]any{"refs": []any{refs[1], refs[0]}}
			done := make(chan any, 1)
			go func() {
				result, err := service.Call(method, params)
				if err != nil {
					done <- err
				} else {
					done <- result
				}
			}()
			for range 2 {
				select {
				case <-started:
				case <-time.After(time.Second):
					t.Fatal("independent Cluster was blocked behind another Cluster")
				}
			}
			close(proceed)
			proceed = nil
			result := <-done
			collection, ok := result.(map[string]any)
			if !ok {
				t.Fatal(result)
			}
			key := "snapshots"
			if method == "jobs.cancel_many" {
				key = "results"
			}
			items := collection[key].([]map[string]any)
			for i, expected := range []map[string]any{refs[1], refs[0]} {
				if RefKey(mapValue(items[i]["ref"])) != RefKey(expected) {
					t.Fatalf("results out of request order: %#v", items)
				}
			}
		})
	}
}

func TestCancelledClusterWaitReturnsBeforeActiveQueryEnds(t *testing.T) {
	started := make(chan string, 2)
	proceed := make(chan struct{})
	service, refs := batchService(t, 1, started, proceed)
	defer close(proceed)
	activeDone := make(chan struct{})
	go func() { defer close(activeDone); _, _ = service.many(context.Background(), refs, "live", nil) }()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	waitDone := make(chan map[string]any, 1)
	go func() { result, _ := service.many(ctx, refs, "live", nil); waitDone <- result }()
	cancel()
	select {
	case result := <-waitDone:
		errors := mapValue(result["coverage"])["errors"].([]map[string]any)
		if len(errors) != 1 || errors[0]["outcome"] != "not_applied" {
			t.Fatalf("errors = %#v", errors)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled query continued waiting for Cluster lock")
	}
}

func TestCancelManyOneFailureStaysAnItemResult(t *testing.T) {
	service, _, ref := observationService(t)
	ref["registry_id"] = "foreign"
	result, err := service.Call("jobs.cancel_many", map[string]any{"refs": []any{ref}})
	if err != nil {
		t.Fatal(err)
	}
	items := result.(map[string]any)["results"].([]map[string]any)
	if len(items) != 1 || mapValue(items[0]["error"])["kind"] != "REGISTRY_MISMATCH" {
		t.Fatalf("items = %#v", items)
	}
}

func TestQueuedCancellationIsNotApplied(t *testing.T) {
	started := make(chan string, 2)
	proceed := make(chan struct{})
	service, refs := batchService(t, 1, started, proceed)
	defer close(proceed)
	go func() { _, _ = service.many(context.Background(), refs, "live", nil) }()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { _, err := service.mutateJob(ctx, refs[0], false); done <- err }()
	select {
	case err := <-done:
		failure, ok := err.(*protocol.Error)
		if !ok || failure.Outcome != "not_applied" {
			t.Fatalf("error = %#v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled mutation remained blocked")
	}
}
