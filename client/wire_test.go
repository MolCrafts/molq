package client

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MolCrafts/molq/protocol"
)

func helloResponse(id any) map[string]any {
	return map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"registry_id": "test-registry"}}
}

func TestMutationDisconnectIsUnknownAndNeverResent(t *testing.T) {
	var mutations atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		if request["method"] == "molq.hello" {
			_ = json.NewEncoder(w).Encode(helloResponse(request["id"]))
			return
		}
		mutations.Add(1)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = conn.Close()
	}))
	defer server.Close()
	wire := New(Options{Endpoint: server.URL, Timeout: time.Second})
	defer wire.Close()
	_, err := wire.Call("jobs.submit", map[string]any{})
	failure, ok := err.(*protocol.Error)
	if !ok || failure.Kind != "OUTCOME_UNKNOWN" || failure.Outcome != "unknown" || mutations.Load() != 1 {
		t.Fatalf("mutations=%d err=%v", mutations.Load(), err)
	}
}

func TestSessionGateHonorsQueuedDeadline(t *testing.T) {
	wire := New(Options{Timeout: time.Second})
	wire.gate <- struct{}{}
	defer func() { <-wire.gate }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := wire.CallContext(ctx, "jobs.submit", nil)
	failure, ok := err.(*protocol.Error)
	if !ok || failure.Kind != "RUNTIME_UNAVAILABLE" || failure.Outcome != "not_applied" || time.Since(start) > time.Second {
		t.Fatal(err)
	}
}

func TestReadFrameAllowsExactLimitAndRejectsOverflow(t *testing.T) {
	for _, size := range []int{maxFrame, maxFrame + 1} {
		wire := New(Options{})
		wire.out = bufio.NewReader(strings.NewReader(strings.Repeat("x", size) + "\n"))
		frame, err := wire.readFrame()
		if size == maxFrame && (err != nil || len(frame) != maxFrame) {
			t.Fatalf("size=%d err=%v", len(frame), err)
		}
		if size > maxFrame && err == nil {
			t.Fatal("accepted oversized response")
		}
	}
}

func TestStructuredStartupErrorIsPreserved(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": nil, "error": map[string]any{
			"code": -32000, "data": protocol.Fail("STATE_SCHEMA_INCOMPATIBLE", "Damaged registry", nil).Data(),
		}})
	}))
	defer server.Close()
	wire := New(Options{Endpoint: server.URL})
	defer wire.Close()
	_, err := wire.Call("clusters.list", nil)
	if failure, ok := err.(*protocol.Error); !ok || failure.Kind != "STATE_SCHEMA_INCOMPATIBLE" {
		t.Fatal(err)
	}
}
