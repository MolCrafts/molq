package main

import (
	"strings"
	"testing"

	"github.com/MolCrafts/molq/protocol"
)

func TestRetiredListNamesJobList(t *testing.T) {
	err := run([]string{"list", "Arrhenius"})
	failure, ok := err.(*protocol.Error)
	if !ok || failure.Kind != "INVALID_INPUT" || !strings.Contains(failure.Message, "molq job list CLUSTER") {
		t.Fatalf("err=%v", err)
	}
}

func TestRetiredClustersNamesCluster(t *testing.T) {
	err := run([]string{"clusters"})
	failure, ok := err.(*protocol.Error)
	if !ok || !strings.Contains(failure.Message, "molq cluster list") {
		t.Fatalf("err=%v", err)
	}
}

func TestJobListTable(t *testing.T) {
	text := renderJobs(map[string]any{"snapshots": []any{
		map[string]any{
			"name": "pbix-npt", "state": "running", "submitted_at": "2026-11-06T19:45:00Z",
			"ref": map[string]any{"native_id": "4927785"},
		},
	}})
	if !strings.Contains(text, "NATIVE_ID") || !strings.Contains(text, "4927785") || !strings.Contains(text, "pbix-npt") {
		t.Fatal(text)
	}
}

func TestEmptyJobListTable(t *testing.T) {
	text := renderJobs(map[string]any{"snapshots": []any{}})
	if !strings.Contains(text, "(none)") {
		t.Fatal(text)
	}
}

func TestClusterListTable(t *testing.T) {
	text := renderClusters([]any{map[string]any{
		"name": "Arrhenius", "scheduler": "slurm", "revision": float64(2),
		"transport": map[string]any{"kind": "ssh", "alias": "Arrhenius"},
	}})
	if !strings.Contains(text, "Arrhenius") || !strings.Contains(text, "ssh Arrhenius") || !strings.Contains(text, "slurm") {
		t.Fatal(text)
	}
	if strings.Contains(text, "2.000") {
		t.Fatal(text)
	}
}

func TestFormatErrorShowsDiagnostic(t *testing.T) {
	text := formatError(protocol.Fail("AUTH_REQUIRED", "SSH authentication was rejected", map[string]any{
		"command": "squeue", "diagnostic": "Permission denied (keyboard-interactive).",
	}))
	for _, part := range []string{"AUTH_REQUIRED", "squeue", "keyboard-interactive"} {
		if !strings.Contains(text, part) {
			t.Fatal(text)
		}
	}
}
