package serve

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/MolCrafts/molq/protocol"
)

func TestFilesRoundTripOnLocalCluster(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("target file scripts are POSIX sh")
	}
	root := t.TempDir()
	service := openTransferService(t, root)
	call := func(method string, params map[string]any) map[string]any {
		t.Helper()
		params["cluster"] = "from"
		result, err := service.Call(method, params)
		if err != nil {
			t.Fatalf("%s: %v", method, err)
		}
		return result.(map[string]any)
	}
	dir := filepath.Join(root, "work", "run")
	call("files.mkdir", map[string]any{"path": dir, "parents": true, "mode": 0o750})
	call("files.mkdir", map[string]any{"path": dir, "exist_ok": true})
	file := filepath.Join(dir, "out name.txt")
	payload := base64.StdEncoding.EncodeToString([]byte("energy -1.5\n"))
	call("files.write", map[string]any{"path": file, "data": payload})
	read := call("files.read", map[string]any{"path": file, "offset": 7})
	if data, _ := base64.StdEncoding.DecodeString(read["data"].(string)); string(data) != "-1.5\n" || read["next_offset"] != 12 {
		t.Fatalf("read = %#v", read)
	}
	stat := call("files.stat", map[string]any{"path": file})
	if stat["exists"] != true || stat["kind"] != "file" || stat["size"] != int64(12) || stat["mtime"].(int64) <= 0 {
		t.Fatalf("stat = %#v", stat)
	}
	if mode := call("files.stat", map[string]any{"path": dir})["mode"]; mode != 0o750 {
		t.Fatalf("directory mode = %o", mode)
	}
	if err := os.Symlink(file, filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	call("files.touch", map[string]any{"path": filepath.Join(dir, ".hidden")})
	names := map[string]string{}
	for _, entry := range call("files.list", map[string]any{"path": dir})["entries"].([]map[string]any) {
		names[entry["name"].(string)] = entry["kind"].(string)
		if entry["name"] == "link" && entry["is_symlink"] != true {
			t.Fatalf("link entry = %#v", entry)
		}
	}
	if len(names) != 3 || names["out name.txt"] != "file" || names[".hidden"] != "file" || names["link"] != "file" {
		t.Fatalf("entries = %#v", names)
	}
	moved := filepath.Join(dir, "moved.txt")
	call("files.rename", map[string]any{"path": file, "target": moved})
	call("files.remove", map[string]any{"path": moved})
	if call("files.stat", map[string]any{"path": moved})["exists"] != false {
		t.Fatal("removed file still exists")
	}
	if stat := call("files.stat", map[string]any{"path": filepath.Join(dir, "link")}); stat["exists"] != false || stat["is_symlink"] != true {
		t.Fatalf("dangling link stat = %#v", stat)
	}
}

func TestFilesReportPreconditionReasons(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("target file scripts are POSIX sh")
	}
	root := t.TempDir()
	service := openTransferService(t, root)
	full := filepath.Join(root, "full")
	if err := os.MkdirAll(full, 0o755); err != nil || os.WriteFile(filepath.Join(full, "x"), nil, 0o600) != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		method string
		params map[string]any
		reason string
	}{
		{"files.list", map[string]any{"path": filepath.Join(root, "missing")}, "not_found"},
		{"files.list", map[string]any{"path": filepath.Join(full, "x")}, "not_directory"},
		{"files.mkdir", map[string]any{"path": full}, "exists"},
		{"files.mkdir", map[string]any{"path": filepath.Join(root, "a", "b")}, "not_found"},
		{"files.remove", map[string]any{"path": full}, "is_directory"},
		{"files.remove", map[string]any{"path": full, "directory": true}, "not_empty"},
		{"files.touch", map[string]any{"path": filepath.Join(full, "x")}, "exists"},
	} {
		check.params["cluster"] = "from"
		_, err := service.Call(check.method, check.params)
		failure, ok := err.(*protocol.Error)
		if !ok || failure.Kind != "FILE_UNAVAILABLE" || failure.Context["reason"] != check.reason || failure.Outcome != "not_applied" {
			t.Fatalf("%s %v error = %#v", check.method, check.params["path"], err)
		}
	}
}

func TestClusterPathFollowsRsyncHomeRule(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skip("no home directory")
	}
	if got, _ := clusterPath(map[string]any{"kind": "local"}, "runs/a/"); got != filepath.ToSlash(filepath.Join(home, "runs", "a"))+"/" {
		t.Fatalf("local relative = %q", got)
	}
	if got, _ := clusterPath(map[string]any{"kind": "ssh", "alias": "hpc"}, "runs"); got != "runs" {
		t.Fatalf("ssh relative = %q", got)
	}
	if !strings.HasPrefix(home, "/") {
		t.Skip("POSIX home only")
	}
}

func TestPermissionBitsReadsSpecialMarkers(t *testing.T) {
	for text, want := range map[string]int{"-rw-r--r--": 0o644, "drwxr-s---": 0o2750, "-rwsr-xr-x@": 0o4755, "drwxrwxrwT": 0o1776} {
		if got := permissionBits(text); got != want {
			t.Fatalf("%s = %o, want %o", text, got, want)
		}
	}
}
