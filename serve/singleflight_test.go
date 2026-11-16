package serve

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/scheduler"
)

type sharedScheduler struct {
	scheduler.Scheduler
	calls     atomic.Int32
	started   chan []string
	proceed   <-chan struct{}
	cancelled chan struct{}
	err       error
}

func (s *sharedScheduler) QueryMany(ctx context.Context, ids []string, _ string) (map[string]scheduler.Job, error) {
	s.calls.Add(1)
	s.started <- ids
	select {
	case <-s.proceed:
		jobs := map[string]scheduler.Job{}
		for _, id := range ids {
			jobs[id] = scheduler.Job{NativeID: id, Incarnation: "submitted-1", State: "running", Source: "queue"}
		}
		return jobs, s.err
	case <-ctx.Done():
		if s.cancelled != nil {
			s.cancelled <- struct{}{}
		}
		return nil, ctx.Err()
	}
}

func sharedQueryService(t *testing.T, proceed <-chan struct{}) (*Service, *sharedScheduler, []map[string]any) {
	t.Helper()
	service, _, ref := observationService(t)
	item := &sharedScheduler{started: make(chan []string, 16), proceed: proceed, cancelled: make(chan struct{}, 16)}
	service.schedulers[ref["cluster_id"].(string)] = item
	other := map[string]any{}
	for key, value := range ref {
		other[key] = value
	}
	other["native_id"] = "2"
	return service, item, []map[string]any{ref, other}
}

func waitQueryWaiters(t *testing.T, service *Service, count int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		service.mu.Lock()
		waiters := 0
		for _, flight := range service.queryFlights {
			waiters += flight.waiters
		}
		service.mu.Unlock()
		if waiters == count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("query waiters did not reach %d", count)
}

func startSharedQuery(service *Service, ctx context.Context, refs []map[string]any) <-chan map[string]any {
	done := make(chan map[string]any, 1)
	go func() {
		result, _ := service.many(ctx, refs, "live", nil)
		done <- result
	}()
	return done
}

func TestIdenticalLiveQueriesShareOnlyInFlightAndPreserveOrder(t *testing.T) {
	proceed := make(chan struct{})
	service, item, refs := sharedQueryService(t, proceed)
	first := startSharedQuery(service, context.Background(), refs)
	<-item.started
	reversed := []map[string]any{refs[1], refs[0], refs[1]}
	second := startSharedQuery(service, context.Background(), reversed)
	waitQueryWaiters(t, service, 2)
	close(proceed)
	<-first
	result := <-second
	if item.calls.Load() != 1 {
		t.Fatalf("native calls = %d", item.calls.Load())
	}
	for index, value := range result["snapshots"].([]map[string]any) {
		if RefKey(mapValue(value["ref"])) != RefKey(reversed[index]) {
			t.Fatalf("snapshot order = %#v", result)
		}
	}
	_, _ = service.many(context.Background(), refs, "live", nil)
	if item.calls.Load() != 2 {
		t.Fatal("completed live query was reused as a cache")
	}
}

func TestLiveQueryKeysSeparateSetsIdentitiesAndRevisions(t *testing.T) {
	proceed := make(chan struct{})
	service, item, refs := sharedQueryService(t, proceed)
	first := startSharedQuery(service, context.Background(), refs[:1])
	<-item.started
	second := startSharedQuery(service, context.Background(), refs)
	waitQueryWaiters(t, service, 2)
	service.mu.Lock()
	groups := len(service.queryFlights)
	service.mu.Unlock()
	if groups != 2 {
		t.Fatalf("distinct native ID sets shared %d query groups", groups)
	}
	close(proceed)
	<-first
	<-second
	if item.calls.Load() != 2 {
		t.Fatalf("native calls = %d", item.calls.Load())
	}
	cluster := map[string]any{"id": refs[0]["cluster_id"], "revision": 1}
	key := queryFlightKey(cluster, refs)
	cluster["revision"] = 2
	if queryFlightKey(cluster, refs) == key {
		t.Fatal("cluster revision was absent from query key")
	}
	cluster["revision"] = 1
	refs[0]["incarnation"] = "submitted-2"
	if queryFlightKey(cluster, refs) == key {
		t.Fatal("native incarnation was absent from query key")
	}
}

func TestCancelledQueryLeaderDoesNotCancelOtherWaiters(t *testing.T) {
	proceed := make(chan struct{})
	service, item, refs := sharedQueryService(t, proceed)
	ctx, cancel := context.WithCancel(context.Background())
	first := startSharedQuery(service, ctx, refs)
	<-item.started
	second := startSharedQuery(service, context.Background(), refs)
	waitQueryWaiters(t, service, 2)
	cancel()
	select {
	case result := <-first:
		if mapValue(result["coverage"])["complete"] != false {
			t.Fatal("cancelled waiter reported a successful query")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled waiter stayed blocked")
	}
	select {
	case <-item.cancelled:
		t.Fatal("leader cancellation interrupted another waiter")
	default:
	}
	close(proceed)
	if mapValue((<-second)["coverage"])["complete"] != true || item.calls.Load() != 1 {
		t.Fatal("surviving waiter did not receive the original query result")
	}
}

func TestAllQueryWaitersCancelNativeWork(t *testing.T) {
	service, item, refs := sharedQueryService(t, make(chan struct{}))
	ctx, cancel := context.WithCancel(context.Background())
	done := startSharedQuery(service, ctx, refs)
	<-item.started
	cancel()
	<-done
	select {
	case <-item.cancelled:
	case <-time.After(time.Second):
		t.Fatal("native query remained active after its last waiter left")
	}
}

func TestQueryErrorIsSharedAndNextLiveRequestRunsAgain(t *testing.T) {
	proceed := make(chan struct{})
	service, item, refs := sharedQueryService(t, proceed)
	item.err = errors.New("native failure")
	first := startSharedQuery(service, context.Background(), refs)
	<-item.started
	second := startSharedQuery(service, context.Background(), refs)
	waitQueryWaiters(t, service, 2)
	close(proceed)
	for _, done := range []<-chan map[string]any{first, second} {
		if mapValue((<-done)["coverage"])["complete"] != false {
			t.Fatal("native failure was dropped")
		}
	}
	_, _ = service.many(context.Background(), refs, "live", nil)
	if item.calls.Load() != 2 {
		t.Fatal("failed completed query was retained")
	}
}

func TestQueryFlightsHaveBoundedAdmission(t *testing.T) {
	service, item, refs := sharedQueryService(t, make(chan struct{}))
	cluster, _, err := service.cluster(refs[0]["cluster_id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	service.queryActive = maxQueryFlights
	_, err = service.sharedQuery(context.Background(), cluster, item, refs)
	failure, ok := err.(*protocol.Error)
	if !ok || failure.Outcome != "not_applied" || item.calls.Load() != 0 {
		t.Fatalf("bounded admission = %#v, calls = %d", err, item.calls.Load())
	}
}
