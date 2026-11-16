package serve

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLegacyArchivePruneRequiresExactDuplicate(t *testing.T) {
	service, root := projectService(t)
	loopbackSSH(t)
	// The legacy transfer conservatively rejects shell-sensitive remote paths.
	remoteParent, err := os.MkdirTemp("", "molq-archive-target-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(remoteParent) })
	remoteParent, err = filepath.EvalSymlinks(remoteParent)
	if err != nil {
		t.Fatal(err)
	}
	binding := map[string]any{"kind": "ssh", "alias": "fixture"}
	id := register(t, service, map[string]any{"name": "legacy", "scheduler": "shell", "transport": binding})
	archive := filepath.Join(root, "collected", id)
	complete, sole, mixed := strings.Repeat("a", 32), strings.Repeat("b", 32), strings.Repeat("c", 32)
	for _, name := range []string{complete, sole, mixed, "." + sole + ".partial", complete + ".staging"} {
		dir := filepath.Join(archive, name)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		files := map[string]string{"stdout": "retained", "owned-v1": "stdout stderr\n"}
		if name == mixed {
			files["checkpoint"] = "unique user data"
		}
		if name == sole {
			files["owned-v1"] = "stdout stderr job.sh launch exit cancel_requested\n"
			files["launch"] = "123\nstart\n"
			files["exit"] = "0\n"
		}
		remote := filepath.Join(remoteParent, ".molcrafts", "molq", "jobs", name, ".molq-control")
		for file, body := range files {
			if err := os.WriteFile(filepath.Join(dir, file), []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			if name == complete {
				if err := os.MkdirAll(remote, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(remote, file), []byte(body), 0o600); err != nil {
					t.Fatal(err)
				}
			}
		}
		if !legacyArchiveName.MatchString(name) {
			continue
		}
		archiveDir, err := os.OpenRoot(dir)
		if err != nil {
			t.Fatal(err)
		}
		entries, err := collectionManifest(context.Background(), archiveDir)
		archiveDir.Close()
		if err != nil {
			t.Fatal(err)
		}
		receipt, err := json.Marshal(collectionReceipt{Remote: remote, Binding: fingerprint("collection", binding), Entries: entries, Cleaned: name == sole})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dir+".receipt.json", receipt, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	before, err := service.Call("maintenance.inspect", map[string]any{"cluster": id})
	if err != nil || len(mapValue(before)["entries"].([]map[string]any)) != 5 {
		t.Fatalf("inspect = %v, %v", before, err)
	}
	result, err := service.Call("maintenance.prune", map[string]any{"cluster": id, "entries": []any{complete, sole, mixed}})
	if err != nil {
		t.Fatal(err)
	}
	entries := mapValue(result)["entries"].([]map[string]any)
	if entries[0]["status"] != "removed" || entries[1]["status"] != "no_verified_duplicate" || entries[2]["status"] != "unverified" {
		t.Fatalf("prune = %#v", entries)
	}
	if _, err := os.Stat(filepath.Join(archive, complete)); !os.IsNotExist(err) {
		t.Fatalf("duplicate status: %v", err)
	}
	for _, file := range []string{filepath.Join(archive, sole, "exit"), filepath.Join(archive, mixed, "checkpoint"),
		filepath.Join(archive, "."+sole+".partial", "stdout"), filepath.Join(archive, complete+".staging", "stdout"),
		filepath.Join(remoteParent, ".molcrafts", "molq", "jobs", complete, ".molq-control", "stdout")} {
		if _, err := os.Stat(file); err != nil {
			t.Fatalf("lost %s: %v", file, err)
		}
	}
}
