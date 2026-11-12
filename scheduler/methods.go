package scheduler

import (
	"context"
	"path"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/transport"
)

type core struct {
	transport transport.Transport
	cluster   map[string]any
}

func unsupported(message string) error {
	return protocol.Fail("SCHEDULER_UNSUPPORTED", message, nil)
}

func explainedTransport(kind string) bool {
	switch kind {
	case "AUTH_REQUIRED", "PERMISSION_DENIED", "TRANSPORT_UNAVAILABLE", "CONNECTION_TIMEOUT":
		return true
	default:
		return false
	}
}

func withCommand(item *protocol.Error, command string) *protocol.Error {
	if item.Context == nil {
		item.Context = map[string]any{}
	}
	if _, ok := item.Context["command"]; !ok {
		item.Context["command"] = command
	}
	return item
}

func (c *core) native(ctx context.Context, argv []string, mutation bool, input []byte, hasInput bool) (string, error) {
	result, err := c.transport.Run(ctx, argv, 30*time.Second, input, hasInput)
	if err != nil {
		item, ok := err.(*protocol.Error)
		if !ok {
			return "", err
		}
		if !mutation && item.Kind == "PARTIAL_FAILURE" {
			return "", err
		}
		dispatched := item.Context["dispatched"] != false
		// Authentication never ran the scheduler command. A dropped connection
		// after dispatch still leaves a mutation outcome unknown.
		if mutation && dispatched && item.Kind != "AUTH_REQUIRED" && item.Kind != "PERMISSION_DENIED" {
			extra := map[string]any{"command": argv[0]}
			if diagnostic, ok := item.Context["diagnostic"]; ok {
				extra["diagnostic"] = diagnostic
			}
			return "", protocol.FailOutcome("OUTCOME_UNKNOWN", "Native mutation outcome is unknown", "unknown", extra)
		}
		if explainedTransport(item.Kind) {
			return "", withCommand(item, argv[0])
		}
		extra := map[string]any{"command": argv[0], "cause": item.Kind, "detail": item.Message}
		if diagnostic, ok := item.Context["diagnostic"]; ok {
			extra["diagnostic"] = diagnostic
		}
		return "", protocol.Fail("SCHEDULER_UNAVAILABLE", "Native scheduler operation unavailable", extra)
	}
	if result.ReturnCode != 0 {
		kind := "SCHEDULER_UNAVAILABLE"
		outcome := "not_applied"
		if mutation && result.ReturnCode == 255 {
			kind = "OUTCOME_UNKNOWN"
			outcome = "unknown"
		} else if mutation {
			kind = "SUBMISSION_REJECTED"
		}
		stderr := result.Stderr
		if len(stderr) > 4096 {
			stderr = stderr[:4096]
		}
		return "", protocol.FailOutcome(kind, "Native scheduler command failed", outcome, map[string]any{
			"command": argv[0], "returncode": result.ReturnCode, "stderr": string(stderr),
		})
	}
	return result.Text(), nil
}

func (c *core) stage(ctx context.Context, spec map[string]any, directory string, validate func(map[string]any) error, render func(map[string]any, string) (string, error)) (string, error) {
	if err := validate(spec); err != nil {
		return "", err
	}
	script, err := render(spec, directory)
	if err != nil {
		return "", err
	}
	target := path.Join(directory, "job.sh")
	if err := c.transport.Write(ctx, target, []byte(script), nil); err != nil {
		return "", err
	}
	return target, nil
}

func (c *core) launch(unit map[string]any, payload string) (string, error) {
	intent := mapAny(unit["launch"])
	if len(intent) == 0 {
		intent = map[string]any{"kind": "direct"}
	}
	if intent["kind"] == "direct" {
		if len(mapAny(unit["placement"])) > 0 {
			return "", unsupported("Direct unit placement requires native execution-step mapping")
		}
		return payload, nil
	}
	launcher, _ := c.cluster["launcher"].(string)
	if launcher != "mpirun" && launcher != "mpiexec" {
		return "", unsupported("MPI requires an explicit mpirun/mpiexec binding for this Scheduler")
	}
	flag := "-np"
	if launcher == "mpiexec" {
		flag = "-n"
	}
	ranks, _ := AsInt(intent["ranks"])
	return launcher + " " + flag + " " + itoa(ranks) + " " + payload, nil
}

func (c *core) validate(spec map[string]any) error {
	units, _ := mapAny(spec["execution"])["units"].([]any)
	for _, item := range units {
		if _, err := c.launch(mapAny(item), "true"); err != nil {
			return err
		}
	}
	return nil
}

func (c *core) accepted(ctx context.Context, query func(context.Context, []string) (map[string]Job, error), identity string) (Job, error) {
	var last error
	for range 4 {
		jobs, err := query(ctx, []string{identity})
		if err != nil {
			return Job{}, protocol.FailOutcome("OUTCOME_UNKNOWN", "Allocation accepted but native identity lookup failed", "unknown", map[string]any{"native_id": identity})
		}
		if job, ok := jobs[identity]; ok && job.Incarnation != "" {
			return job, nil
		}
		last = nil
		time.Sleep(50 * time.Millisecond)
	}
	_ = last
	return Job{}, protocol.FailOutcome("OUTCOME_UNKNOWN", "Allocation accepted but native identity evidence unavailable", "unknown", map[string]any{"native_id": identity})
}

func nonempty(value any) bool {
	switch item := value.(type) {
	case nil:
		return false
	case string:
		return item != ""
	case map[string]any:
		return len(item) > 0
	case []any:
		return len(item) > 0
	default:
		return true
	}
}
