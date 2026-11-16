package watch

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MolCrafts/molq/client"
	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/rpc"
	"github.com/MolCrafts/molq/serve"
)

func TestOnceOnEmptyRegistryPrintsNothing(t *testing.T) {
	service, err := serve.Open(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	var out bytes.Buffer
	err = Run(context.Background(), publicWire(t, service), Options{Interval: time.Second, Once: true, Out: &out})
	if err != nil {
		t.Fatal(err)
	}
	if out.Len() != 0 {
		t.Fatalf("output = %s", out.String())
	}
}

func TestSlowNotificationDoesNotBlockSamplingOrShutdown(t *testing.T) {
	started := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		started <- struct{}{}
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	send, stop := notifications(ctx, server.URL)
	send(map[string]any{"state": "succeeded"})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("notification did not start")
	}
	done := make(chan struct{})
	go func() {
		for range 1024 {
			send(map[string]any{"state": "succeeded"})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sampling blocked behind slow recipient")
	}
	cancel()
	done = make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("notification prevented shutdown")
	}
}

func TestReportErrorPrintsARepeatedFailureOnce(t *testing.T) {
	var out bytes.Buffer
	reported := map[string]string{}
	err := protocol.Fail("SCHEDULER_UNAVAILABLE", "ssh failed", nil)
	reportError(&out, reported, "Arrhenius", err)
	reportError(&out, reported, "Arrhenius", err)
	if strings.Count(out.String(), "SCHEDULER_UNAVAILABLE") != 1 {
		t.Fatalf("output = %s", out.String())
	}
	delete(reported, "Arrhenius")
	reportError(&out, reported, "Arrhenius", err)
	if strings.Count(out.String(), "SCHEDULER_UNAVAILABLE") != 2 {
		t.Fatalf("output = %s", out.String())
	}
}

func TestShellWatchOnceIgnoresUnsupportedHistory(t *testing.T) {
	root := t.TempDir()
	service, err := serve.Open(filepath.Join(root, "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	if _, err := service.Call("clusters.register", map[string]any{
		"name": "local", "scheduler": "shell",
	}); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err = Run(context.Background(), publicWire(t, service), Options{Once: true, Clusters: []string{"local"}, ProjectRoot: filepath.ToSlash(root), Out: &out})
	if err != nil {
		t.Fatal(err)
	}
	if out.Len() != 0 {
		t.Fatalf("output = %s", out.String())
	}
}

func publicWire(t *testing.T, service *serve.Service) *client.Wire {
	t.Helper()
	handler, err := rpc.Handler(service, "0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	wire := client.New(client.Options{Endpoint: server.URL, Token: "0123456789abcdef"})
	t.Cleanup(func() { _ = wire.Close() })
	return wire
}

type baselineCaller struct{ calls []string }

func (c *baselineCaller) CallContext(_ context.Context, method string, _ map[string]any) (any, error) {
	c.calls = append(c.calls, method)
	return map[string]any{"cursor": "baseline", "resync": true, "changes": []any{}}, nil
}
func TestResyncDoesNotInventTerminalEvents(t *testing.T) {
	var out bytes.Buffer
	caller := &baselineCaller{}
	err := Run(context.Background(), caller, Options{Once: true, Clusters: []string{"local"}, Out: &out})
	if err != nil || len(caller.calls) != 1 || caller.calls[0] != "jobs.observe" || out.Len() != 0 {
		t.Fatalf("calls=%v output=%s err=%v", caller.calls, out.String(), err)
	}
}

type failedCaller struct{}

func (failedCaller) CallContext(context.Context, string, map[string]any) (any, error) {
	return nil, protocol.Fail("CLUSTER_NOT_FOUND", "missing cluster", nil)
}

func TestOnceFailureAlreadyReported(t *testing.T) {
	for _, clusters := range [][]string{nil, {"first", "second"}} {
		var out bytes.Buffer
		err := Run(context.Background(), failedCaller{}, Options{Once: true, Clusters: clusters, Out: &out})
		var reported *ReportedError
		if !errors.As(err, &reported) {
			t.Fatalf("error = %v", err)
		}
		count := 1
		if len(clusters) > 0 {
			count = len(clusters)
		}
		if strings.Count(out.String(), "CLUSTER_NOT_FOUND") != count {
			t.Fatalf("output = %s", out.String())
		}
	}
}
