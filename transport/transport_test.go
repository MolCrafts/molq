package transport

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/MolCrafts/molq/protocol"
)

func requirePOSIX(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("local POSIX target execution")
	}
}

func TestLocalRunCancellationAfterDispatch(t *testing.T) {
	requirePOSIX(t)
	marker := filepath.Join(t.TempDir(), "started")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := (Local{}).Run(ctx, []string{"sh", "-c", `printf started > "$1"; exec sleep 30`, "sh", marker}, 10*time.Second, nil, false)
		done <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("target command did not start")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		var failure *protocol.Error
		if !errors.As(err, &failure) || failure.Kind != "TRANSPORT_UNAVAILABLE" || failure.Context["dispatched"] != true {
			t.Fatalf("dispatched cancellation = %#v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled command did not stop")
	}
}

func TestLocalRunCanceledBeforeDispatch(t *testing.T) {
	requirePOSIX(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (Local{}).Run(ctx, []string{"sh", "-c", "exit 0"}, time.Second, nil, false)
	var failure *protocol.Error
	if !errors.As(err, &failure) || failure.Context["dispatched"] != false {
		t.Fatalf("undispatched cancellation = %#v", err)
	}
}

func TestLocalRunOverflowPreservesDetachedAllocation(t *testing.T) {
	requirePOSIX(t)
	marker := filepath.Join(t.TempDir(), "finished")
	_, err := (Local{}).Run(context.Background(), []string{"bash", "-c", `set -m
nohup sh -c 'sleep 0.2; printf finished > "$1"' sh "$1" </dev/null >/dev/null 2>&1 &
while :; do printf '%01024d' 0; done`, "bash", marker}, 5*time.Second, nil, false)
	var failure *protocol.Error
	if !errors.As(err, &failure) || failure.Kind != "PARTIAL_FAILURE" {
		t.Fatalf("output overflow = %#v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("detached allocation did not survive launch-command cleanup")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
