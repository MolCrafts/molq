package serve

import (
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MolCrafts/molq/registry"
	"github.com/MolCrafts/molq/scheduler"
)

// Snapshot is one canonical observation. Unavailable is never terminal.
func Snapshot(ref map[string]any, job *scheduler.Job, failure map[string]any) map[string]any {
	state := "unknown"
	source := "absent"
	raw := ""
	var exit any
	var name, comment, submitted, started, ended any
	if job != nil {
		state = job.State
		source = job.Source
		raw = job.RawState
		exit = job.ExitCode
		name = textValue(job.Name)
		comment = textValue(job.Comment)
		submitted = textValue(job.SubmittedAt)
		started = textValue(job.StartedAt)
		ended = textValue(job.EndedAt)
	} else if failure != nil {
		source = "unavailable"
	}
	result := map[string]any{
		"ref": ref, "state": state, "terminal": scheduler.Terminal(state) && failure == nil,
		"observed_at": nowRFC3339(),
		"freshness":   map[string]any{"status": "live", "age_seconds": 0},
		"source":      source, "raw_state": raw, "exit_code": exit,
		"name": name, "comment": comment, "submitted_at": submitted,
		"started_at": started, "ended_at": ended,
	}
	if job != nil && len(job.NativeDiagnostic) > 0 {
		result["native_diagnostic"] = job.NativeDiagnostic
	}
	if job != nil && job.FailureReason != "" && scheduler.Terminal(state) {
		result["failure_reason"] = job.FailureReason
	}
	if failure != nil {
		result["error"] = failure
		result["freshness"] = map[string]any{"status": "unavailable", "age_seconds": 0}
		result["terminal"] = false
	}
	return result
}

// Collection computes shared completion for one result set.
func Collection(snapshots []map[string]any, failures []map[string]any) map[string]any {
	if failures == nil {
		failures = []map[string]any{}
	}
	if snapshots == nil {
		snapshots = []map[string]any{}
	}
	complete := len(failures) == 0
	for _, item := range snapshots {
		if item["source"] == "absent" || item["source"] == "unavailable" {
			complete = false
		}
	}
	terminal := complete && len(snapshots) > 0
	successful := true
	for _, item := range snapshots {
		if item["terminal"] != true {
			terminal = false
		}
		if item["state"] != "succeeded" {
			successful = false
		}
	}
	var success any
	if terminal {
		success = successful
	}
	return map[string]any{
		"snapshots": snapshots,
		"coverage":  map[string]any{"complete": complete, "errors": failures},
		"completion": map[string]any{
			"all_confirmed_terminal": terminal,
			"successful":             success,
		},
	}
}

// RefKey keeps registry, cluster, native ID and incarnation in every cache key.
func RefKey(ref map[string]any) string {
	return string(mustJSON([]string{
		stringField(ref["registry_id"]), stringField(ref["cluster_id"]),
		stringField(ref["native_id"]), stringField(ref["incarnation"]),
	}))
}

// Observation is disposable cache, baseline and event memory.
type Observation struct {
	mu            sync.Mutex
	maxEntries    int
	maxBytes      int
	cache         map[string]cacheItem
	cacheOrder    []string
	cacheBytes    int
	baselines     map[string]baselineRecord
	baselineOrder []string
	baselineBytes int
	events        []map[string]any
	eventSizes    []int
	eventBytes    int
	sequence      int
	instance      string
}

type cacheItem struct {
	size  int
	at    time.Time
	value map[string]any
}

type baselineRecord struct {
	size  int
	at    time.Time
	scope string
	items map[string]map[string]any
}

// NewObservation starts an empty in-memory view.
func NewObservation(maxEntries int) *Observation {
	if maxEntries == 0 {
		maxEntries = 4096
	}
	return &Observation{
		maxEntries: maxEntries, maxBytes: 16777216,
		cache: map[string]cacheItem{}, baselines: map[string]baselineRecord{},
		instance: newHex(),
	}
}

func newHex() string {
	value, err := registry.NewHex()
	if err != nil {
		return "observation"
	}
	return value
}

// SetMaxEntries changes the cache entry bound and evicts overflow.
func (o *Observation) SetMaxEntries(entries int) {
	if entries <= 0 {
		entries = 4096
	}
	o.mu.Lock()
	o.maxEntries = entries
	o.evictCache()
	o.mu.Unlock()
}

// Clear drops scoped memory after a destination or configuration change.
func (o *Observation) Clear() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.cache = map[string]cacheItem{}
	o.cacheOrder = nil
	o.cacheBytes = 0
	o.baselines = map[string]baselineRecord{}
	o.baselineOrder = nil
	o.baselineBytes = 0
	o.events = nil
	o.eventSizes = nil
	o.eventBytes = 0
	o.sequence = 0
	o.instance = newHex()
}

// Remember caches an observation that has native evidence.
func (o *Observation) Remember(value map[string]any) {
	if value["source"] == "absent" || value["source"] == "unavailable" {
		return
	}
	stored := cloneSnapshot(value)
	identity := RefKey(mapValue(stored["ref"]))
	size := len(mustJSON(stored))
	o.mu.Lock()
	defer o.mu.Unlock()
	if previous, ok := o.cache[identity]; ok {
		o.cacheBytes -= previous.size
	} else {
		o.cacheOrder = append(o.cacheOrder, identity)
	}
	o.cache[identity] = cacheItem{at: time.Now(), value: stored, size: size}
	o.cacheBytes += size
	o.evictCache()
}

// Cached reads one bounded snapshot. It is not current truth.
func (o *Observation) Cached(ref map[string]any, maxAge *time.Duration) map[string]any {
	identity := RefKey(ref)
	o.mu.Lock()
	item, ok := o.cache[identity]
	o.mu.Unlock()
	if !ok {
		return nil
	}
	age := time.Since(item.at).Seconds()
	if maxAge != nil && time.Since(item.at) > *maxAge {
		return nil
	}
	value := cloneSnapshot(item.value)
	value["freshness"] = map[string]any{"status": "cached", "age_seconds": age}
	return value
}

func (o *Observation) evictCache() {
	for len(o.cacheOrder) > o.maxEntries || o.cacheBytes > o.maxBytes {
		if len(o.cacheOrder) == 0 {
			return
		}
		identity := o.cacheOrder[0]
		o.cacheOrder = o.cacheOrder[1:]
		if item, ok := o.cache[identity]; ok {
			o.cacheBytes -= item.size
			delete(o.cache, identity)
		}
	}
}

func textValue(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func nowRFC3339() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.999999999Z")
}

func stringField(value any) string {
	text, _ := value.(string)
	return text
}

func mustJSON(value any) []byte {
	encoded, _ := json.Marshal(value)
	return encoded
}

func cloneSnapshot(value map[string]any) map[string]any {
	copied := make(map[string]any, len(value))
	for key, item := range value {
		copied[key] = cloneObservationValue(item)
	}
	return copied
}

// Stored JSON values are immutable; readers copy after releasing the lock.
func cloneObservationValue(value any) any {
	switch item := value.(type) {
	case map[string]any:
		return cloneSnapshot(item)
	case []any:
		out := make([]any, len(item))
		for i, entry := range item {
			out[i] = cloneObservationValue(entry)
		}
		return out
	case []map[string]any:
		out := make([]map[string]any, len(item))
		for i, entry := range item {
			out[i] = cloneSnapshot(entry)
		}
		return out
	case nil, string, bool, float64, int, int64, uint64, json.Number:
		return item
	default:
		// Native snapshots can contain pointer-valued exit codes. JSON
		// normalization avoids retaining mutable pointers into stored state.
		return deepCopy(item)
	}
}

func cloneBaseline(items map[string]map[string]any) map[string]map[string]any {
	out := make(map[string]map[string]any, len(items))
	for key, value := range items {
		out[key] = cloneSnapshot(value)
	}
	return out
}

// Observe computes one scoped diff and keeps only a bounded baseline.
func (o *Observation) Observe(result map[string]any, scope, cursor string) map[string]any {
	// Prepare a private baseline and its byte budget before taking the lock.
	complete := mapValue(result["coverage"])["complete"] == true
	current := map[string]map[string]any{}
	size := len(scope) + 2
	if complete {
		snapshots, _ := result["snapshots"].([]map[string]any)
		for _, value := range snapshots {
			current[RefKey(mapValue(value["ref"]))] = cloneSnapshot(value)
		}
		size = len(mustJSON(current)) + len(scope)
	}
	out := cloneSnapshot(result)
	o.mu.Lock()
	prior, ok := o.baselines[cursor]
	resync := !ok || prior.scope != scope || time.Since(prior.at) > time.Hour
	baseline := map[string]map[string]any{}
	if !resync {
		baseline = prior.items
		if !complete {
			size = prior.size
		}
	}
	var changes []map[string]any
	if complete {
		if !resync {
			for identity, value := range current {
				old := baseline[identity]
				if old == nil || old["state"] != value["state"] {
					o.sequence++
					event := map[string]any{
						"cursor":      o.instance + ":" + strconv.Itoa(o.sequence),
						"observed_at": nowRFC3339(), "ref": value["ref"],
						"previous_state": nil, "state": value["state"], "terminal": value["terminal"],
					}
					if old != nil {
						event["previous_state"] = old["state"]
					}
					o.events = append(o.events, event)
					eventSize := len(mustJSON(event))
					o.eventSizes = append(o.eventSizes, eventSize)
					o.eventBytes += eventSize
					for len(o.events) > 512 || o.eventBytes > o.maxBytes/4 {
						o.eventBytes -= o.eventSizes[0]
						o.events = o.events[1:]
						o.eventSizes = o.eventSizes[1:]
					}
					changes = append(changes, event)
				}
			}
		}
		baseline = current
	}
	token := cursor
	if resync {
		token = newHex()
	}
	if previous, exists := o.baselines[token]; exists {
		o.baselineBytes -= previous.size
	} else {
		o.baselineOrder = append(o.baselineOrder, token)
	}
	o.baselines[token] = baselineRecord{at: time.Now(), scope: scope, items: baseline, size: size}
	o.baselineBytes += size
	for len(o.baselineOrder) > 64 || o.baselineBytes > o.maxBytes {
		if len(o.baselineOrder) == 0 {
			break
		}
		removed := o.baselineOrder[0]
		o.baselineOrder = o.baselineOrder[1:]
		if item, exists := o.baselines[removed]; exists {
			o.baselineBytes -= item.size
			delete(o.baselines, removed)
		}
	}
	if changes == nil {
		changes = []map[string]any{}
	}
	o.mu.Unlock()
	// Events may remain in the ring after returning; expose private copies.
	out["changes"] = cloneObservationValue(changes)
	out["cursor"] = token
	out["resync"] = resync
	return out
}

// Since reads the event ring or asks the caller to resync.
func (o *Observation) Since(cursor string) map[string]any {
	o.mu.Lock()
	start := o.sequence
	resync := cursor == ""
	if cursor != "" {
		instance, sequence, ok := parseCursor(cursor)
		start = sequence
		resync = !ok || instance != o.instance || start > o.sequence || start < o.sequence-len(o.events)
	}
	events := []map[string]any{}
	if !resync {
		for _, event := range o.events {
			_, sequence, ok := parseCursor(stringField(event["cursor"]))
			if ok && sequence > start {
				events = append(events, event)
			}
		}
	}
	next := o.instance + ":" + strconv.Itoa(o.sequence)
	o.mu.Unlock()
	return map[string]any{"events": cloneObservationValue(events), "cursor": next, "resync": resync}
}

// Baseline returns one stored scope, used when an observation cursor is still valid.
func (o *Observation) Baseline(cursor string) (string, map[string]map[string]any, bool) {
	o.mu.Lock()
	item, ok := o.baselines[cursor]
	o.mu.Unlock()
	if !ok {
		return "", nil, false
	}
	return item.scope, cloneBaseline(item.items), true
}

func parseCursor(cursor string) (string, int, bool) {
	instance, sequence, ok := strings.Cut(cursor, ":")
	if !ok {
		return "", 0, false
	}
	number, err := strconv.Atoi(sequence)
	if err != nil {
		return "", 0, false
	}
	return instance, number, true
}
