// Package scheduler owns native resource, script, dependency and state mapping.
package scheduler

import "encoding/json"

// Job is one native allocation observation.
type Job struct {
	NativeID      string
	Members       []Job
	FailureReason string
	Incarnation   string
	State         string
	Source        string
	RawState      string
	ExitCode      *int
	Workdir       *string
	Name          *string
	StdoutPath    *string
	StderrPath    *string
	Comment       *string
	SubmittedAt   *string
	StartedAt     *string
	EndedAt       *string
}

// Terminal reports a confirmed finished state.
func Terminal(state string) bool {
	switch state {
	case "succeeded", "failed", "cancelled", "timed_out":
		return true
	default:
		return false
	}
}

// AsInt accepts JSON numbers stored as int, int64 or float64.
func AsInt(value any) (int, bool) {
	switch item := value.(type) {
	case int:
		return item, true
	case int64:
		return int(item), true
	case float64:
		if item != float64(int(item)) {
			return 0, false
		}
		return int(item), true
	case json.Number:
		number, err := item.Int64()
		return int(number), err == nil
	default:
		return 0, false
	}
}

func strPtr(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
