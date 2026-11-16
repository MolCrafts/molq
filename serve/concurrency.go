package serve

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/MolCrafts/molq/protocol"
)

// At most eight native Cluster operations run concurrently in one serve process.
// Batch workers are finite and disappear as soon as their request finishes.
const batchConcurrency = 8

func runBatches(count int, execute func(int)) {
	var next atomic.Int64
	var workers sync.WaitGroup
	for range min(count, batchConcurrency) {
		workers.Go(func() {
			for {
				index := int(next.Add(1)) - 1
				if index >= count {
					return
				}
				execute(index)
			}
		})
	}
	workers.Wait()
}

func (s *Service) acquire(ctx context.Context, id string) (func(), error) {
	s.ensure()
	failure := func() error {
		return protocol.Fail("TRANSPORT_UNAVAILABLE", "Cluster operation cancelled before native dispatch", map[string]any{
			"cluster_id": id, "dispatched": false,
		})
	}
	if ctx.Err() != nil {
		return nil, failure()
	}
	s.mu.Lock()
	gate := s.locks[id]
	if gate == nil {
		gate = make(chan struct{}, 1)
		s.locks[id] = gate
	}
	s.mu.Unlock()
	select {
	case gate <- struct{}{}:
	case <-ctx.Done():
		return nil, failure()
	}
	select {
	case s.nativeSlots <- struct{}{}:
	case <-ctx.Done():
		<-gate
		return nil, failure()
	}
	release := func() {
		<-s.nativeSlots
		<-gate
	}
	if ctx.Err() != nil {
		release()
		return nil, failure()
	}
	return release, nil
}
