//go:build !windows

package serve

import (
	"path/filepath"
	"syscall"
	"testing"
)

// files.mkdir follows pathlib.Path.mkdir: the requested mode, default 0o777,
// is masked by the target's umask, so nothing is world-writable by default.
func TestMkdirModeIsMaskedByTheUmask(t *testing.T) {
	previous := syscall.Umask(0o027)
	defer syscall.Umask(previous)
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
	cases := []struct {
		name   string
		params map[string]any
		want   int
	}{
		{"default", map[string]any{}, 0o750},
		{"explicit", map[string]any{"mode": 0o777}, 0o750},
		{"narrower", map[string]any{"mode": 0o700}, 0o700},
		{"parents", map[string]any{"parents": true}, 0o750},
	}
	for _, c := range cases {
		dir := filepath.Join(root, c.name)
		if c.name == "parents" {
			dir = filepath.Join(root, "a", "b", c.name)
		}
		c.params["path"] = dir
		call("files.mkdir", c.params)
		if mode := call("files.stat", map[string]any{"path": dir})["mode"]; mode != c.want {
			t.Errorf("%s: mode = %o, want %o", c.name, mode, c.want)
		}
	}
	if mode := call("files.stat", map[string]any{"path": filepath.Join(root, "a")})["mode"]; mode != 0o750 {
		t.Errorf("created parent: mode = %o, want 750", mode)
	}
}
