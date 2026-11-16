package serve

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/scheduler"
)

// In-flight sharing is disposable request state, never a Job session or cache.
const maxQueryFlights = 128

type queryFlight struct {
	done    chan struct{}
	cancel  context.CancelFunc
	waiters int
	jobs    map[string]scheduler.Job
	err     error
}

func queryFlightKey(cluster map[string]any, refs []map[string]any) string {
	identities := make([]string, 0, len(refs))
	for _, ref := range refs {
		identities = append(identities, RefKey(ref))
	}
	sort.Strings(identities)
	identities = compactIdentities(identities)
	key, _ := json.Marshal([]any{cluster["id"], cluster["revision"], identities})
	return string(key)
}

func compactIdentities(identities []string) []string {
	result := identities[:0]
	for _, identity := range identities {
		if len(result) == 0 || result[len(result)-1] != identity {
			result = append(result, identity)
		}
	}
	return result
}

func queryCancelled(id, message string) error {
	return protocol.Fail("TRANSPORT_UNAVAILABLE", message, map[string]any{"cluster_id": id, "dispatched": false})
}

func (s *Service) sharedQuery(ctx context.Context, cluster map[string]any, item scheduler.Scheduler, refs []map[string]any) (map[string]scheduler.Job, error) {
	id := stringField(cluster["id"])
	if ctx.Err() != nil {
		return nil, queryCancelled(id, "Query cancelled before native dispatch")
	}
	key := queryFlightKey(cluster, refs)
	s.mu.Lock()
	flight := s.queryFlights[key]
	if flight == nil {
		if s.queryActive >= maxQueryFlights {
			s.mu.Unlock()
			return nil, queryCancelled(id, "Too many in-flight native query groups")
		}
		if s.queryFlights == nil {
			s.queryFlights = map[string]*queryFlight{}
		}
		sharedCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 120*time.Second)
		flight = &queryFlight{done: make(chan struct{}), cancel: cancel}
		s.queryFlights[key] = flight
		s.queryActive++
		ids := nativeIDs(refs)
		project := stringField(refs[0]["project_root"])
		go s.executeQuery(sharedCtx, key, id, item, ids, project, flight)
	}
	flight.waiters++
	s.mu.Unlock()
	defer s.leaveQuery(key, flight)
	select {
	case <-ctx.Done():
		return nil, queryCancelled(id, "Query waiter cancelled")
	case <-flight.done:
		if ctx.Err() != nil {
			return nil, queryCancelled(id, "Query waiter cancelled")
		}
		return flight.jobs, flight.err
	}
}

func (s *Service) executeQuery(ctx context.Context, key, id string, item scheduler.Scheduler, ids []string, project string, flight *queryFlight) {
	defer flight.cancel()
	release, err := s.acquire(ctx, id)
	if err == nil {
		flight.jobs, err = item.QueryMany(ctx, ids, project)
		release()
	}
	flight.err = err
	s.mu.Lock()
	if s.queryFlights[key] == flight {
		delete(s.queryFlights, key)
	}
	s.queryActive--
	close(flight.done)
	s.mu.Unlock()
}

func (s *Service) leaveQuery(key string, flight *queryFlight) {
	s.mu.Lock()
	defer s.mu.Unlock()
	flight.waiters--
	if flight.waiters == 0 {
		if s.queryFlights[key] == flight {
			delete(s.queryFlights, key)
		}
		flight.cancel()
	}
}
