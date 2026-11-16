package scheduler

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/transport"
)

func TestDispatchedMutationFailureHasUnknownOutcome(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("local POSIX target execution")
	}
	for _, test := range []struct {
		name    string
		command string
		timeout time.Duration
	}{
		{"output_overflow", "while :; do printf '%01024d' 0; done", 5 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), test.timeout)
			defer cancel()
			item := core{transport: transport.Local{}}
			_, err := item.native(ctx, []string{"sh", "-c", test.command}, true, nil, false)
			var failure *protocol.Error
			if !errors.As(err, &failure) || failure.Kind != "OUTCOME_UNKNOWN" || failure.Outcome != "unknown" {
				t.Fatalf("dispatched mutation failure = %#v", err)
			}
		})
	}
}
