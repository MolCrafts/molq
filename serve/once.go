package serve

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"

	"github.com/MolCrafts/molq/protocol"
)

type remembered struct {
	at          time.Time
	fingerprint string
	result      any
	err         error
	ready       chan struct{}
}

func (s *Service) callDeduped(method string, params map[string]any) (any, error) {
	key := stringField(params["request_key"])
	if key == "" || method != "jobs.cancel" {
		return s.invoke(method, params)
	}
	fingerprint := fingerprint(method, params)
	s.mu.Lock()
	if s.operations == nil {
		s.operations = map[string]*remembered{}
	}
	s.pruneOperations()
	if existing := s.operations[key]; existing != nil {
		if existing.fingerprint != fingerprint {
			s.mu.Unlock()
			return nil, protocol.Fail("CONFLICT", "request_key was already used for a different operation", nil)
		}
		ready := existing.ready
		s.mu.Unlock()
		<-ready
		return existing.result, existing.err
	}
	if len(s.operations) >= 1024 {
		s.mu.Unlock()
		return nil, protocol.Fail("STATE_BUSY", "Bounded operation-result store is full", nil)
	}
	operation := &remembered{at: time.Now(), fingerprint: fingerprint, ready: make(chan struct{})}
	s.operations[key] = operation
	s.mu.Unlock()
	operation.result, operation.err = s.invoke(method, params)
	close(operation.ready)
	return operation.result, operation.err
}

func (s *Service) pruneOperations() {
	now := time.Now()
	for key, operation := range s.operations {
		select {
		case <-operation.ready:
			if now.Sub(operation.at) > 10*time.Minute {
				delete(s.operations, key)
			}
		default:
		}
	}
}

func fingerprint(method string, params map[string]any) string {
	encoded, _ := json.Marshal(map[string]any{"method": method, "params": canonical(params)})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func canonical(value any) any {
	switch item := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(item))
		for key := range item {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out := make(map[string]any, len(keys))
		for _, key := range keys {
			out[key] = canonical(item[key])
		}
		return out
	case []any:
		out := make([]any, len(item))
		for i, child := range item {
			out[i] = canonical(child)
		}
		return out
	default:
		return item
	}
}
