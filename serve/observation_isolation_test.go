package serve

import (
	"strconv"
	"sync"
	"testing"
)

func TestObservationResponsesDoNotExposeStoredValues(t *testing.T) {
	o := NewObservation(8)
	value := benchmarkSnapshots(1)[0]
	ref := cloneSnapshot(mapValue(value["ref"]))
	identity := RefKey(ref)
	o.Remember(value)
	first := o.Observe(Collection([]map[string]any{value}, nil), "cluster", "")
	cursor := first["cursor"].(string)
	eventsCursor := o.Since("")["cursor"].(string)
	// Mutating an accepted input must not change its remembered baseline.
	value["state"] = "failed"
	mapValue(value["ref"])["native_id"] = "replaced"
	cached := o.Cached(ref, nil)
	if cached["state"] != "running" || mapValue(cached["ref"])["native_id"] != "0" {
		t.Fatalf("input changed cache: %#v", cached)
	}
	cached["state"] = "failed"
	mapValue(cached["ref"])["native_id"] = "changed"
	_, baseline, _ := o.Baseline(cursor)
	if baseline[identity]["state"] != "running" {
		t.Fatal("input changed baseline")
	}
	baseline[identity]["state"] = "failed"
	mapValue(baseline[identity]["ref"])["native_id"] = "changed"
	unchanged := benchmarkSnapshots(1)[0]
	same := o.Observe(Collection([]map[string]any{unchanged}, nil), "cluster", cursor)
	if len(same["changes"].([]map[string]any)) != 0 {
		t.Fatal("external mutation created a false event")
	}
	unchanged["state"] = "succeeded"
	unchanged["terminal"] = true
	changed := o.Observe(Collection([]map[string]any{unchanged}, nil), "cluster", cursor)
	changes := changed["changes"].([]map[string]any)
	if len(changes) != 1 || changes[0]["previous_state"] != "running" {
		t.Fatalf("changes = %#v", changes)
	}
	changes[0]["state"] = "failed"
	mapValue(changes[0]["ref"])["native_id"] = "changed"
	events := o.Since(eventsCursor)["events"].([]map[string]any)
	if events[0]["state"] != "succeeded" || mapValue(events[0]["ref"])["native_id"] != "0" {
		t.Fatalf("response changed ring: %#v", events)
	}
	events[0]["state"] = "failed"
	mapValue(events[0]["ref"])["native_id"] = "changed"
	if got := o.Since(eventsCursor)["events"].([]map[string]any)[0]; got["state"] != "succeeded" || mapValue(got["ref"])["native_id"] != "0" {
		t.Fatal("event response changed stored event")
	}
}

func TestObservationByteBudgetsStayExact(t *testing.T) {
	o := NewObservation(2)
	o.maxBytes = 8192
	values := benchmarkSnapshots(20)
	for _, value := range values {
		o.Remember(value)
	}
	cacheBytes := 0
	for _, item := range o.cache {
		cacheBytes += len(mustJSON(item.value))
	}
	if len(o.cache) > 2 || cacheBytes != o.cacheBytes {
		t.Fatalf("cache budget = %d, calculated %d", o.cacheBytes, cacheBytes)
	}
	result := Collection(values[:1], nil)
	for range 70 {
		o.Observe(result, "cluster", "")
	}
	baselineBytes := 0
	for _, item := range o.baselines {
		baselineBytes += len(mustJSON(item.items)) + len(item.scope)
	}
	if len(o.baselines) > 64 || baselineBytes != o.baselineBytes || baselineBytes > o.maxBytes {
		t.Fatalf("baseline budget = %d, calculated %d", o.baselineBytes, baselineBytes)
	}
	cursor := o.Observe(result, "cluster", "")["cursor"].(string)
	for i := range 600 {
		value := cloneSnapshot(values[0])
		value["state"] = strconv.Itoa(i)
		o.Observe(Collection([]map[string]any{value}, nil), "cluster", cursor)
	}
	eventBytes := 0
	for _, event := range o.events {
		eventBytes += len(mustJSON(event))
	}
	if len(o.events) > 512 || eventBytes != o.eventBytes || eventBytes > o.maxBytes/4 {
		t.Fatalf("event budget = %d, calculated %d", o.eventBytes, eventBytes)
	}
	o.Clear()
	if o.cacheBytes != 0 || o.baselineBytes != 0 || o.eventBytes != 0 || len(o.eventSizes) != 0 {
		t.Fatal("clear retained byte accounting")
	}
}

func TestObservationConcurrentReadersOwnTheirCopies(t *testing.T) {
	o := NewObservation(8)
	value := benchmarkSnapshots(1)[0]
	ref := mapValue(value["ref"])
	identity := RefKey(ref)
	result := Collection([]map[string]any{value}, nil)
	cursor := o.Observe(result, "cluster", "")["cursor"].(string)
	eventsCursor := o.Since("")["cursor"].(string)
	o.Remember(value)
	var calls sync.WaitGroup
	for range 8 {
		calls.Go(func() {
			for range 128 {
				o.Cached(ref, nil)["state"] = "reader"
				_, baseline, _ := o.Baseline(cursor)
				baseline[identity]["state"] = "reader"
				for _, event := range o.Since(eventsCursor)["events"].([]map[string]any) {
					event["state"] = "reader"
				}
			}
		})
	}
	for i := range 128 {
		next := cloneSnapshot(value)
		next["state"] = strconv.Itoa(i)
		o.Remember(next)
		o.Observe(Collection([]map[string]any{next}, nil), "cluster", cursor)
	}
	calls.Wait()
}

func TestRefKeyDoesNotCollideOnSeparators(t *testing.T) {
	first := map[string]any{"registry_id": "registry", "cluster_id": "cluster", "native_id": "1|2", "incarnation": "3"}
	second := map[string]any{"registry_id": "registry", "cluster_id": "cluster", "native_id": "1", "incarnation": "2|3"}
	if RefKey(first) == RefKey(second) {
		t.Fatal("different native identities share an observation key")
	}
}
