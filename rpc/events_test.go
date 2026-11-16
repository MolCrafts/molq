package rpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MolCrafts/molq/serve"
)

func changedService(t *testing.T) (*serve.Service, string) {
	t.Helper()
	service := open(t)
	result, err := service.Call("events.subscribe", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	return service, result.(map[string]any)["cursor"].(string)
}

func TestEventStreamCatchesRegistrationGapAndStopsOnWriteFailure(t *testing.T) {
	service, cursor := changedService(t)
	// A config/destination change invalidates the cursor while no listener exists.
	_, err := service.Call("clusters.register", map[string]any{
		"name": "local", "scheduler": "shell",
	})
	if err != nil {
		t.Fatal(err)
	}
	updates, stop := service.Listen()
	defer stop()
	done := make(chan struct{})
	writes := make(chan any, 1)
	go func() {
		streamSocket(context.Background(), service, cursor, updates, func(value any) error {
			writes <- value
			return errors.New("closed output")
		})
		close(done)
	}()
	select {
	case value := <-writes:
		if value.(map[string]any)["params"].(map[string]any)["resync"] != true {
			t.Fatal(value)
		}
	case <-time.After(time.Second):
		t.Fatal("missed change during subscription registration")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stream retained after write failure")
	}
}

func TestEventStreamStopsWhenListenerCloses(t *testing.T) {
	service, cursor := changedService(t)
	updates, stop := service.Listen()
	done := make(chan struct{})
	go func() {
		streamSocket(context.Background(), service, cursor, updates, func(any) error { return nil })
		close(done)
	}()
	stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stream retained after session close")
	}
}
