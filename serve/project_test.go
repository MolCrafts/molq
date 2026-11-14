package serve

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/MolCrafts/molq/protocol"
)

func projectService(t *testing.T) (*Service, string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("POSIX target fixture")
	}
	root := t.TempDir()
	service, err := Open(filepath.Join(root, "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { service.Close() })
	return service, root
}

func register(t *testing.T, service *Service, definition map[string]any) string {
	t.Helper()
	registered, err := service.Call("clusters.register", definition)
	if err != nil {
		t.Fatal(err)
	}
	return registered.(map[string]any)["id"].(string)
}

// loopbackSSH puts an ssh on PATH that runs the remote command locally, the
// way OpenSSH hands it to the login shell, for both molq and rsync callers.
// The alias "unreachable" fails the way an unresolvable host does.
func loopbackSSH(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("POSIX target fixture")
	}
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("rsync is not installed")
	}
	bin := t.TempDir()
	script := `#!/bin/sh
while [ $# -gt 0 ]; do
  case "$1" in
    -o) shift 2 ;;
    --) shift; break ;;
    -*) shift ;;
    *) break ;;
  esac
done
if [ "$1" = unreachable ]; then
  echo "ssh: Could not resolve hostname unreachable" >&2
  exit 255
fi
shift
exec sh -c "$*"
`
	if err := os.WriteFile(filepath.Join(bin, "ssh"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func TestProjectScopeAndPreviewDoNotCreateDirectories(t *testing.T) {
	service, root := projectService(t)
	id := register(t, service, map[string]any{"name": "shell", "scheduler": "shell"})
	project, outside := filepath.Join(root, "project"), filepath.Join(root, "outside")
	for _, dir := range []string{project, outside} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(outside, filepath.Join(project, "escape")); err != nil {
		t.Fatal(err)
	}
	spec := map[string]any{"execution": map[string]any{"units": []any{
		map[string]any{"id": "main", "command": map[string]any{"kind": "argv", "argv": []any{"true"}}},
	}}}
	params := map[string]any{"cluster": id, "spec": spec, "project_root": filepath.Join(project, "escape"), "project_scope": project}
	if _, err := service.Call("jobs.submit", params); err == nil {
		t.Fatal("submitted outside project scope")
	}
	params["project_root"] = project
	if _, err := service.Call("jobs.preview", params); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(project, ".molq")); err != nil {
		t.Fatal(err)
	}
	_, err := service.Call("jobs.submit", params)
	if failure, ok := err.(*protocol.Error); !ok || failure.Kind != "FILE_UNAVAILABLE" {
		t.Fatalf("aliased reserved directory accepted: %v", err)
	}
	if entries, err := os.ReadDir(outside); err != nil || len(entries) != 0 {
		t.Fatalf("outside project changed: %v %v", entries, err)
	}
	if entries, err := os.ReadDir(project); err != nil || len(entries) != 2 {
		t.Fatalf("preview or failed submit changed project: %v %v", entries, err)
	}
}
