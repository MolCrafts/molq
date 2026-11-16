package serve

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/MolCrafts/molq/protocol"
)

func TestLocalTransferCopiesFileOutsideTargetRoot(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("rsync is not installed")
	}
	root := t.TempDir()
	service := openTransferService(t, root)
	source := filepath.Join(root, "source")
	destination := filepath.Join(root, "destination")
	if err := os.MkdirAll(source, 0o755); err != nil || os.MkdirAll(destination, 0o755) != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "note.txt"), []byte("copied"), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := service.Call("files.transfer", map[string]any{
		"sources":     []any{map[string]any{"cluster": "from", "path": filepath.Join(source, "note.txt")}},
		"destination": map[string]any{"cluster": "to", "path": filepath.Join(destination, "note.txt")},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := result.(map[string]any)
	if got["outcome"] != "applied" {
		t.Fatalf("result = %#v", result)
	}
	body, err := os.ReadFile(filepath.Join(destination, "note.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "copied" {
		t.Fatalf("body = %q", body)
	}
}

func TestTransferPassesRsyncOptionsVerbatim(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("rsync is not installed")
	}
	root := t.TempDir()
	service := openTransferService(t, root)
	source := filepath.Join(root, "source")
	destination := filepath.Join(root, "destination")
	if err := os.MkdirAll(filepath.Join(source, "build"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(destination, "build"), 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(path, body string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(source, "keep.txt"), "newer")
	write(filepath.Join(source, "build", "artifact"), "nope")
	write(filepath.Join(destination, "keep.txt"), "old")
	write(filepath.Join(destination, "stale.txt"), "gone")
	write(filepath.Join(destination, "build", "kept"), "stay")
	if _, err := service.Call("files.transfer", map[string]any{
		"sources":     []any{map[string]any{"path": source + "/"}},
		"destination": map[string]any{"cluster": "to", "path": destination + "/"},
		"options":     []any{"-a", "--delete", "--exclude=build/"},
	}); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(destination, "keep.txt"))
	if err != nil || string(body) != "newer" {
		t.Fatalf("keep.txt = %q, %v", body, err)
	}
	if _, err := os.Stat(filepath.Join(destination, "stale.txt")); !os.IsNotExist(err) {
		t.Fatalf("stale.txt err = %v", err)
	}
	kept, err := os.ReadFile(filepath.Join(destination, "build", "kept"))
	if err != nil || string(kept) != "stay" {
		t.Fatalf("build/kept = %q, %v", kept, err)
	}
	if _, err := os.Stat(filepath.Join(destination, "build", "artifact")); !os.IsNotExist(err) {
		t.Fatalf("build/artifact err = %v", err)
	}
}

func TestTransferRejectsUnsafeOperandsAndOwnedOptions(t *testing.T) {
	root := t.TempDir()
	service := openTransferService(t, root)
	destination := map[string]any{"cluster": "to", "path": filepath.Join(root, "destination") + "/"}
	for name, params := range map[string]map[string]any{
		"relative serve-host path": {"sources": []any{map[string]any{"path": "note.txt"}}, "destination": destination},
		"filesystem root":          {"sources": []any{map[string]any{"cluster": "from", "path": "/"}}, "destination": destination},
		"transport-owned option": {
			"sources": []any{map[string]any{"path": root + "/"}}, "destination": destination, "options": []any{"--rsh=ssh -p 2"},
		},
		"detached option value": {
			"sources": []any{map[string]any{"path": root + "/"}}, "destination": destination, "options": []any{"--exclude"},
		},
	} {
		_, err := service.Call("files.transfer", params)
		failure, ok := err.(*protocol.Error)
		if !ok || failure.Kind != "INVALID_INPUT" {
			t.Fatalf("%s error = %#v", name, err)
		}
	}
}

func TestTransferImpliesNoDelete(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("rsync is not installed")
	}
	root := t.TempDir()
	service := openTransferService(t, root)
	source, destination := filepath.Join(root, "source"), filepath.Join(root, "destination")
	for _, dir := range []string{source, destination} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(destination, "stale.txt"), []byte("kept"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Call("files.transfer", map[string]any{
		"sources": []any{map[string]any{"path": source + "/"}}, "destination": map[string]any{"path": destination + "/"},
		"options": []any{"-a"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(destination, "stale.txt")); err != nil {
		t.Fatalf("rsync without --delete removed stale.txt: %v", err)
	}
}

func openTransferService(t *testing.T, root string) *Service {
	t.Helper()
	service, err := Open(filepath.Join(root, "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { service.Close() })
	for _, name := range []string{"from", "to"} {
		if _, err := service.Call("clusters.register", map[string]any{
			"name": name, "scheduler": "shell",
		}); err != nil {
			t.Fatal(err)
		}
	}
	return service
}

func TestSSHTransferKeepsRshBeforeOperands(t *testing.T) {
	loopbackSSH(t)
	root := t.TempDir()
	service := openTransferService(t, root)
	if _, err := service.Call("clusters.register", map[string]any{
		"name": "remote", "scheduler": "shell",
		"transport": map[string]any{"kind": "ssh", "alias": "fixture"},
	}); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, "note.txt")
	if err := os.WriteFile(source, []byte("over ssh"), 0o600); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "remote-note.txt")
	if _, err := service.Call("files.transfer", map[string]any{
		"sources":     []any{map[string]any{"path": source}},
		"destination": map[string]any{"cluster": "remote", "path": destination},
	}); err != nil {
		t.Fatal(err)
	}
	if body, err := os.ReadFile(destination); err != nil || string(body) != "over ssh" {
		t.Fatalf("remote copy = %q, %v", body, err)
	}
}

// A scoped directory transfer walks the links below each operand with
// find -exec. GNU find refuses a second "{}" in one -exec ... + clause, and
// the validator passes its own text as an argument, so that text must not
// spell "{}" or every scoped directory transfer fails on a Linux target.
func TestScopedDirectoryTransferWithALinkUnderTheRoot(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("rsync is not installed")
	}
	root := t.TempDir()
	service := openTransferService(t, root)
	source := filepath.Join(root, "source")
	destination := filepath.Join(root, "destination")
	if err := os.MkdirAll(source, 0o755); err != nil || os.MkdirAll(destination, 0o755) != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "note.txt"), []byte("copied"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("note.txt", filepath.Join(source, "alias.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(source, "note.txt"), filepath.Join(destination, "alias.txt")); err != nil {
		t.Fatal(err)
	}
	result, err := service.Call("files.transfer", map[string]any{
		"sources":     []any{map[string]any{"cluster": "from", "path": source + "/", "root": root}},
		"destination": map[string]any{"cluster": "to", "path": destination + "/", "root": root},
		"options":     []any{"-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.(map[string]any)["outcome"] != "applied" {
		t.Fatalf("result = %#v", result)
	}
	body, err := os.ReadFile(filepath.Join(destination, "note.txt"))
	if err != nil || string(body) != "copied" {
		t.Fatalf("body = %q, err = %v", body, err)
	}
}

func TestScopedSSHOperandRejectsRemoteShellExpansion(t *testing.T) {
	remote := map[string]any{"kind": "ssh", "alias": "fixture"}
	local := map[string]any{"kind": "local"}
	for _, name := range []string{"/output/*.xyz", "/output/with space", "/output/a$(echo x)"} {
		_, _, _, err := transferRoute(
			[]location{{transport: remote, path: name, root: "/output"}},
			location{transport: local, path: "/destination"},
		)
		if err == nil {
			t.Fatalf("unsafe scoped operand accepted: %q", name)
		}
	}
}
