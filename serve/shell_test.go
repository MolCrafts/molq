package serve

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestShellSubmitObserveAndLogs(t *testing.T) {
	service, root := projectService(t)
	registered, err := service.Call("clusters.register", map[string]any{
		"name": "local", "scheduler": "shell",
	})
	if err != nil {
		t.Fatal(err)
	}
	cluster := registered.(map[string]any)
	submitted, err := service.Call("jobs.submit", map[string]any{
		"cluster": cluster["id"], "project_root": root,
		"spec": map[string]any{
			"execution": map[string]any{
				"units": []any{map[string]any{
					"id":      "main",
					"command": map[string]any{"kind": "argv", "argv": []any{"echo", "molq09"}},
				}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ref := submitted.(map[string]any)["ref"]
	deadline := time.Now().Add(5 * time.Second)
	var observed map[string]any
	for time.Now().Before(deadline) {
		result, err := service.Call("jobs.observe", map[string]any{"refs": []any{ref}})
		if err != nil {
			t.Fatal(err)
		}
		observed = result.(map[string]any)
		if observed["completion"].(map[string]any)["all_confirmed_terminal"] == true {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if observed["completion"].(map[string]any)["all_confirmed_terminal"] != true {
		t.Fatalf("not terminal: %#v", observed)
	}
	logs, err := service.Call("logs.read", map[string]any{"ref": ref, "stream": "stdout"})
	if err != nil {
		t.Fatal(err)
	}
	if text, _ := logs.(map[string]any)["text"].(string); text != "molq09\n" && text != "molq09" {
		t.Fatalf("logs = %q", text)
	}
}

func TestSSHShellProjectReceiptsAfterRestart(t *testing.T) {
	service, root := projectService(t)
	loopbackSSH(t)
	id := register(t, service, map[string]any{"name": "shell", "scheduler": "shell", "transport": map[string]any{"kind": "ssh", "alias": "fixture"}})
	project := filepath.Join(root, "project with spaces")
	if err := os.Mkdir(project, 0o700); err != nil {
		t.Fatal(err)
	}
	spec := map[string]any{"execution": map[string]any{
		"units": []any{map[string]any{"id": "main", "command": map[string]any{
			"kind": "argv", "argv": []any{"sh", "-c", "echo ready; sleep 15"},
		}}},
	}}
	params := map[string]any{"cluster": id, "spec": spec, "project_root": project, "request_key": "ssh-run"}
	result, err := service.Call("jobs.submit", params)
	if err != nil {
		t.Fatal(err)
	}
	ref := mapValue(mapValue(result)["ref"])
	dir := filepath.Join(project, ".molq", "jobs", stringField(ref["run_id"]))
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	resumed, err := Open(filepath.Join(root, "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close()
	list, err := resumed.Call("jobs.list", map[string]any{"cluster": id, "project_root": project})
	if err != nil || len(mapValue(list)["snapshots"].([]map[string]any)) != 1 {
		t.Fatalf("project list = %v, %v", list, err)
	}
	readyBy := time.Now().Add(5 * time.Second)
	for {
		chunk, err := resumed.Call("logs.read", map[string]any{"ref": ref})
		if err == nil && mapValue(chunk)["text"] == "ready\n" {
			break
		}
		if time.Now().After(readyBy) {
			t.Fatalf("Shell did not become ready: %v, %v", chunk, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if _, err := resumed.Call("jobs.cancel", map[string]any{"ref": ref}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		status, err := resumed.Call("jobs.get", map[string]any{"ref": ref})
		if err != nil {
			t.Fatal(err)
		}
		if state := mapValue(status)["snapshots"].([]map[string]any)[0]["state"]; state == "cancelled" {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("Shell cancellation remained %v", state)
		}
		time.Sleep(25 * time.Millisecond)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 4 {
		t.Fatalf("project receipts/logs: %v, %v", entries, err)
	}
	logs, err := resumed.Call("logs.read", map[string]any{"ref": ref})
	if err != nil || mapValue(logs)["text"] != "ready\n" {
		t.Fatalf("logs = %v, %v", logs, err)
	}
	if _, err := os.Stat(filepath.Join(root, "collected")); !os.IsNotExist(err) {
		t.Fatalf("observation created an archive: %v", err)
	}
	if err := os.RemoveAll(project); err != nil {
		t.Fatal(err)
	}
	replayed, err := resumed.Call("jobs.submit", params)
	if err != nil || !reflect.DeepEqual(mapValue(replayed)["ref"], ref) {
		t.Fatalf("project deletion lost submission evidence: %#v, %v", replayed, err)
	}
	if _, err := resumed.Call("logs.read", map[string]any{"ref": ref}); err == nil {
		t.Fatal("deleted project still has readable logs")
	}
}
