package serve

import (
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/scheduler"
)

var rfc3339 = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$`)

func parseHistoryTime(value string) (time.Time, error) {
	if !rfc3339.MatchString(value) {
		return time.Time{}, protocol.Fail("INVALID_INPUT", "History time must be RFC3339 with timezone", nil)
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, protocol.Fail("INVALID_INPUT", "Invalid history timestamp", nil)
	}
	return parsed.UTC(), nil
}

func historyWindow(params map[string]any) (time.Time, time.Time, error) {
	until := time.Now().UTC()
	var err error
	if value, ok := params["until"].(string); ok {
		until, err = parseHistoryTime(value)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
	}
	since := until.Add(-24 * time.Hour)
	if value, ok := params["since"].(string); ok {
		since, err = parseHistoryTime(value)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
	}
	if !since.Before(until) {
		return time.Time{}, time.Time{}, protocol.Fail("INVALID_INPUT", "History since must precede until", nil)
	}
	return since, until, nil
}

func filterSnapshots(values []map[string]any, params map[string]any) []map[string]any {
	states, _ := params["states"].([]any)
	name, named := params["name"].(string)
	var out []map[string]any
	for _, value := range values {
		if len(states) > 0 && !contains(states, value["state"]) {
			continue
		}
		if named && !sameText(value["name"], name) {
			continue
		}
		out = append(out, value)
	}
	if out == nil {
		out = []map[string]any{}
	}
	return out
}

func historyResult(jobs []scheduler.Job, refOf func(scheduler.Job) (map[string]any, error), params map[string]any, since, until time.Time) map[string]any {
	var values []map[string]any
	var failures []map[string]any
	for _, job := range jobs {
		if !scheduler.Terminal(job.State) {
			if completing(job) {
				failures = append(failures, protocol.Fail("JOB_IDENTITY_UNVERIFIABLE", "Historical entry is still completing", map[string]any{"native_id": job.NativeID}).Data())
			}
			continue
		}
		if job.Incarnation == "" || job.EndedAt == nil || *job.EndedAt == "" {
			failures = append(failures, protocol.Fail("JOB_IDENTITY_UNVERIFIABLE", "Historical entry lacks identity/end evidence", map[string]any{"native_id": job.NativeID}).Data())
			continue
		}
		ended, err := parseHistoryTime(*job.EndedAt)
		if err != nil {
			failures = append(failures, protocol.Fail("JOB_IDENTITY_UNVERIFIABLE", "Historical entry has an unreadable end time", map[string]any{"native_id": job.NativeID}).Data())
			continue
		}
		if ended.Before(since) || !ended.Before(until) {
			continue
		}
		ref, err := refOf(job)
		if err != nil {
			failures = append(failures, asData(err))
			continue
		}
		values = append(values, Snapshot(ref, &job, nil))
	}
	values = filterSnapshots(values, params)
	unique := map[string]map[string]any{}
	var order []string
	for _, value := range values {
		identity := RefKey(mapValue(value["ref"]))
		if _, ok := unique[identity]; !ok {
			order = append(order, identity)
		}
		unique[identity] = value
	}
	ordered := make([]map[string]any, 0, len(order))
	for _, identity := range order {
		ordered = append(ordered, unique[identity])
	}
	sort.Slice(ordered, func(i, j int) bool {
		left := stringField(ordered[i]["ended_at"])
		right := stringField(ordered[j]["ended_at"])
		if left == right {
			return RefKey(mapValue(ordered[i]["ref"])) < RefKey(mapValue(ordered[j]["ref"]))
		}
		return left < right
	})
	limit := 100
	if value, ok := asInt(params["limit"]); ok {
		limit = value
	}
	if len(ordered) > limit {
		failures = append(failures, protocol.Fail("PARTIAL_FAILURE", "History exceeds limit; narrow the time window", map[string]any{"limit": limit}).Data())
		ordered = ordered[:limit]
	}
	result := Collection(ordered, failures)
	result["query"] = map[string]any{
		"since": since.Format(time.RFC3339Nano), "until": until.Format(time.RFC3339Nano),
		"time_field": "ended_at", "account_scope": "current_target_user", "source": "accounting",
		"observed_at": nowRFC3339(), "freshness": map[string]any{"status": "live", "age_seconds": 0},
		"limit": limit,
	}
	return result
}

func sameText(value any, want string) bool {
	switch item := value.(type) {
	case string:
		return item == want
	case *string:
		return item != nil && *item == want
	default:
		return false
	}
}

func completing(job scheduler.Job) bool {
	raw := strings.TrimSpace(job.RawState)
	if raw == "" {
		return false
	}
	for _, flag := range strings.Split(raw, ",") {
		if fields := strings.Fields(flag); len(fields) != 0 && strings.TrimRight(fields[0], "+") == "COMPLETING" {
			return true
		}
	}
	return false
}

func contains(values []any, want any) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func asData(err error) map[string]any {
	if item, ok := err.(*protocol.Error); ok {
		return item.Data()
	}
	return protocol.Fail("INTERNAL_ERROR", err.Error(), nil).Data()
}
