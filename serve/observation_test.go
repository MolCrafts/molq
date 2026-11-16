package serve

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/MolCrafts/molq/scheduler"
)

type observationScheduler struct {
	scheduler.Scheduler
	job     scheduler.Job
	queries int
	lists   int
	listed  bool
}

func (s *observationScheduler) QueryMany(context.Context, []string, string) (map[string]scheduler.Job, error) {
	s.queries++
	return map[string]scheduler.Job{s.job.NativeID: s.job}, nil
}

func (s *observationScheduler) List(context.Context, string) ([]scheduler.Job, error) {
	s.lists++
	if !s.listed {
		return []scheduler.Job{}, nil
	}
	return []scheduler.Job{s.job}, nil
}

func observationService(t *testing.T) (*Service, *observationScheduler, map[string]any) {
	t.Helper()
	root := t.TempDir()
	service, err := Open(filepath.Join(root, "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	registered, err := service.Call("clusters.register", map[string]any{
		"name": "local", "scheduler": "shell",
	})
	if err != nil {
		t.Fatal(err)
	}
	cluster := registered.(map[string]any)
	item := &observationScheduler{listed: true, job: scheduler.Job{
		NativeID: "1", Incarnation: "submitted-1", State: "running", Source: "queue",
	}}
	service.schedulers[cluster["id"].(string)] = item
	ref, err := service.ref(cluster, item.job)
	if err != nil {
		t.Fatal(err)
	}
	return service, item, ref
}

func TestClusterObserveReusesListAndQueriesOnlyMissingRefs(t *testing.T) {
	service, item, ref := observationService(t)
	params := map[string]any{"cluster": ref["cluster_id"]}
	first, err := service.Call("jobs.observe", params)
	if err != nil {
		t.Fatal(err)
	}
	if item.lists != 1 || item.queries != 0 {
		t.Fatalf("initial list = %d, query = %d", item.lists, item.queries)
	}
	params["cursor"] = first.(map[string]any)["cursor"]
	item.listed = false
	item.job.State = "succeeded"
	second, err := service.Call("jobs.observe", params)
	if err != nil {
		t.Fatal(err)
	}
	if item.lists != 2 || item.queries != 1 {
		t.Fatalf("final list = %d, query = %d", item.lists, item.queries)
	}
	changes := second.(map[string]any)["changes"].([]map[string]any)
	if len(changes) != 1 || changes[0]["state"] != "succeeded" {
		t.Fatalf("changes = %#v", changes)
	}
}

func TestBoundedObservationHonorsMaxAge(t *testing.T) {
	for _, test := range []struct {
		name      string
		maxAge    any
		configAge float64
		cached    bool
	}{
		{name: "expired", maxAge: float64(1)},
		{name: "fresh", maxAge: float64(3600), cached: true},
		{name: "zero", maxAge: float64(0)},
		{name: "fractional", maxAge: float64(0.5)},
		{name: "integer", maxAge: int64(1)},
		{name: "default"},
		{name: "configured", configAge: 3600, cached: true},
		{name: "request_overrides_config", configAge: 3600, maxAge: float64(1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, item, ref := observationService(t)
			if test.configAge != 0 {
				if err := service.UseConfig(filepath.Join(t.TempDir(), "config.json")); err != nil {
					t.Fatal(err)
				}
				if _, err := service.Call("config.set", map[string]any{"key": "cache_max_age", "value": test.configAge}); err != nil {
					t.Fatal(err)
				}
			}
			service.obs.Remember(Snapshot(ref, &item.job, nil))
			cached := service.obs.cache[RefKey(ref)]
			cached.at = time.Now().Add(-time.Minute)
			service.obs.cache[RefKey(ref)] = cached
			item.job.State = "succeeded"
			params := map[string]any{"ref": ref, "consistency": "bounded"}
			if test.maxAge != nil {
				params["max_age"] = test.maxAge
			}
			result, err := service.Call("jobs.get", params)
			if err != nil {
				t.Fatal(err)
			}
			snapshot := result.(map[string]any)["snapshots"].([]map[string]any)[0]
			state, queries := "succeeded", 1
			if test.cached {
				state, queries = "running", 0
			}
			if snapshot["state"] != state || item.queries != queries {
				t.Fatalf("state = %v, queries = %d; want %s, %d", snapshot["state"], item.queries, state, queries)
			}
		})
	}
}

func TestLiveAndCachedObservationKeepTheirSemantics(t *testing.T) {
	service, item, ref := observationService(t)
	service.obs.Remember(Snapshot(ref, &item.job, nil))
	item.job.State = "succeeded"
	for _, test := range []struct {
		consistency string
		state       string
		queries     int
	}{
		{consistency: "cached", state: "running", queries: 0},
		{consistency: "live", state: "succeeded", queries: 1},
	} {
		result, err := service.Call("jobs.get", map[string]any{
			"ref": ref, "consistency": test.consistency, "max_age": float64(0),
		})
		if err != nil {
			t.Fatal(err)
		}
		snapshot := result.(map[string]any)["snapshots"].([]map[string]any)[0]
		if snapshot["state"] != test.state || item.queries != test.queries {
			t.Fatalf("%s: state = %v, queries = %d", test.consistency, snapshot["state"], item.queries)
		}
	}
}

func TestObserveNotifiesOnlyRealChanges(t *testing.T) {
	service, item, ref := observationService(t)
	updates, stop := service.Listen()
	defer stop()
	params := map[string]any{"refs": []any{ref}}
	first, err := service.Call("jobs.observe", params)
	if err != nil {
		t.Fatal(err)
	}
	params["cursor"] = first.(map[string]any)["cursor"]
	select {
	case <-updates:
		t.Fatal("first baseline woke subscriber")
	default:
	}
	if _, err := service.Call("jobs.observe", params); err != nil {
		t.Fatal(err)
	}
	select {
	case <-updates:
		t.Fatal("unchanged state woke subscriber")
	default:
	}
	item.job.State = "succeeded"
	result, err := service.Call("jobs.observe", params)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.(map[string]any)["changes"].([]map[string]any)) != 1 {
		t.Fatal("expected one state change")
	}
	select {
	case <-updates:
	default:
		t.Fatal("observed state change did not wake subscriber")
	}
}

func TestConcurrentFirstServiceCalls(t *testing.T) {
	service, err := Open(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	var calls sync.WaitGroup
	start := make(chan struct{})
	for range 64 {
		calls.Go(func() {
			<-start
			if _, err := service.Call("clusters.list", map[string]any{}); err != nil {
				t.Error(err)
			}
		})
	}
	close(start)
	calls.Wait()
}
