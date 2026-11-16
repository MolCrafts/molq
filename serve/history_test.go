package serve

import (
	"testing"
	"time"

	"github.com/MolCrafts/molq/scheduler"
)

func historyRef(job scheduler.Job) (map[string]any, error) {
	return map[string]any{
		"registry_id": "registry", "cluster_id": "cluster",
		"native_id": job.NativeID, "incarnation": job.Incarnation,
	}, nil
}

func TestHistoryNameMatchesSnapshotString(t *testing.T) {
	name := "atv-cuda-nddo"
	ended := "2026-10-12T12:00:00Z"
	code := 8
	job := scheduler.Job{
		NativeID: "3711469", Incarnation: "2026-10-12T11:39:01Z",
		State: "failed", Source: "accounting", RawState: "FAILED",
		ExitCode: &code, Name: &name, EndedAt: &ended,
	}
	since := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	result := historyResult([]scheduler.Job{job}, historyRef, map[string]any{"name": name}, since, since.Add(48*time.Hour))
	snaps := result["snapshots"].([]map[string]any)
	if len(snaps) != 1 || snaps[0]["name"] != name {
		t.Fatalf("%#v", result["snapshots"])
	}
	if _, ok := snaps[0]["name"].(string); !ok {
		t.Fatalf("name type = %T", snaps[0]["name"])
	}
}

func TestHistoryNameExcludesOtherJobs(t *testing.T) {
	name := "other"
	ended := "2026-10-12T12:00:00Z"
	job := scheduler.Job{
		NativeID: "1", Incarnation: "2026-10-12T11:00:00Z",
		State: "succeeded", Source: "accounting", RawState: "COMPLETED",
		Name: &name, EndedAt: &ended,
	}
	since := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	result := historyResult([]scheduler.Job{job}, historyRef, map[string]any{"name": "atv-cuda-nddo"}, since, since.Add(48*time.Hour))
	if len(result["snapshots"].([]map[string]any)) != 0 {
		t.Fatal(result["snapshots"])
	}
	if result["coverage"].(map[string]any)["complete"] != true {
		t.Fatal(result["coverage"])
	}
}

func TestHistoryFilterAcceptsPointerName(t *testing.T) {
	name := "atv-cuda-nddo"
	out := filterSnapshots([]map[string]any{{"name": &name, "state": "failed"}}, map[string]any{"name": name})
	if len(out) != 1 {
		t.Fatalf("%#v", out)
	}
}

func TestHistoryCompletingWithoutEndIsCoverageError(t *testing.T) {
	job := scheduler.Job{
		NativeID: "3711469", Incarnation: "2026-10-12T11:39:01Z",
		State: "unknown", Source: "accounting", RawState: "COMPLETING",
	}
	since := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	result := historyResult([]scheduler.Job{job}, historyRef, map[string]any{}, since, since.Add(48*time.Hour))
	errors := result["coverage"].(map[string]any)["errors"].([]map[string]any)
	if len(result["snapshots"].([]map[string]any)) != 0 || len(errors) != 1 || errors[0]["message"] != "Historical entry is still completing" {
		t.Fatalf("%#v", result)
	}
}

func TestHistoryUnreadableEndIsCoverageError(t *testing.T) {
	ended := "yesterday"
	job := scheduler.Job{
		NativeID: "3711469", Incarnation: "2026-10-12T11:39:01Z",
		State: "failed", Source: "accounting", RawState: "FAILED", EndedAt: &ended,
	}
	since := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	result := historyResult([]scheduler.Job{job}, historyRef, map[string]any{}, since, since.Add(48*time.Hour))
	errors := result["coverage"].(map[string]any)["errors"].([]map[string]any)
	if len(errors) != 1 || errors[0]["message"] != "Historical entry has an unreadable end time" {
		t.Fatalf("%#v", result["coverage"])
	}
}

func TestHistoryOutOfWindowStaysSilent(t *testing.T) {
	ended := "2026-10-01T12:00:00Z"
	running := scheduler.Job{NativeID: "2", Incarnation: "2026-10-12T11:00:00Z", State: "running", RawState: "RUNNING"}
	finished := scheduler.Job{
		NativeID: "1", Incarnation: "2026-10-01T11:00:00Z",
		State: "succeeded", RawState: "COMPLETED", EndedAt: &ended,
	}
	since := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	result := historyResult([]scheduler.Job{running, finished}, historyRef, map[string]any{}, since, since.Add(48*time.Hour))
	errors := result["coverage"].(map[string]any)["errors"].([]map[string]any)
	if len(result["snapshots"].([]map[string]any)) != 0 || len(errors) != 0 || result["coverage"].(map[string]any)["complete"] != true {
		t.Fatalf("%#v", result)
	}
}
