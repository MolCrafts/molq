package serve

import (
	"strconv"
	"testing"

	"github.com/MolCrafts/molq/scheduler"
)

func benchmarkSnapshots(count int) []map[string]any {
	values := make([]map[string]any, count)
	for i := range values {
		values[i] = Snapshot(map[string]any{"registry_id": "registry", "cluster_id": "cluster", "native_id": strconv.Itoa(i), "incarnation": "submitted"}, &scheduler.Job{State: "running", Source: "queue"}, nil)
	}
	return values
}

func BenchmarkObservationCacheParallel(b *testing.B) {
	observation := NewObservation(4096)
	values := benchmarkSnapshots(1024)
	for _, value := range values {
		observation.Remember(value)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			value := values[i%len(values)]
			observation.Remember(value)
			observation.Cached(mapValue(value["ref"]), nil)
			i++
		}
	})
}

func BenchmarkObservationBaseline1024(b *testing.B) {
	observation := NewObservation(4096)
	result := Collection(benchmarkSnapshots(1024), nil)
	cursor := observation.Observe(result, "cluster", "")["cursor"].(string)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		observation.Observe(result, "cluster", cursor)
	}
}
