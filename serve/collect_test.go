package serve

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/MolCrafts/molq/scheduler"
)

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

func TestTerminalJobDirectoryMovesToServeHost(t *testing.T) {
	loopbackSSH(t)
	root := t.TempDir()
	service, err := Open(filepath.Join(root, "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	t.Setenv("HOME", filepath.Join(root, "home"))
	target := filepath.Join(root, "home", ".molcrafts", "molq", "jobs")
	registered, err := service.Call("clusters.register", map[string]any{
		"name": "remote", "scheduler": "shell",
		"transport": map[string]any{"kind": "ssh", "alias": "fixture"},
	})
	if err != nil {
		t.Fatal(err)
	}
	cluster := registered.(map[string]any)
	submitted, err := service.Call("jobs.submit", map[string]any{
		"cluster": cluster["id"],
		"spec": map[string]any{"execution": map[string]any{"units": []any{map[string]any{
			"id":      "main",
			"command": map[string]any{"kind": "shell", "command": "echo molq09; echo done > result.txt"},
		}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ref := submitted.(map[string]any)["ref"].(map[string]any)
	waitTerminal(t, service, ref)

	local := filepath.Join(root, "collected", cluster["id"].(string), ref["native_id"].(string))
	for _, name := range []string{"job.sh", "stdout", "launch", "exit", "result.txt"} {
		if _, err := os.Stat(filepath.Join(local, name)); err != nil {
			t.Fatalf("collected %s: %v", name, err)
		}
	}
	if entries, err := os.ReadDir(target); err != nil || len(entries) != 0 {
		t.Fatalf("target keeps %v, %v", entries, err)
	}
	again, err := service.Call("jobs.get", map[string]any{"ref": ref})
	if err != nil {
		t.Fatal(err)
	}
	if state := again.(map[string]any)["snapshots"].([]map[string]any)[0]["state"]; state != "succeeded" {
		t.Fatalf("collected receipt state = %v", again)
	}
	logs, err := service.Call("logs.read", map[string]any{"ref": ref, "stream": "stdout"})
	if err != nil {
		t.Fatal(err)
	}
	if text := logs.(map[string]any)["text"]; text != "molq09\n" {
		t.Fatalf("logs = %q", text)
	}
}

func TestCollectableRequiresSubmittedTerminalDirectory(t *testing.T) {
	archive := "/local/collected/cluster"
	dir := func(value string) *string { return &value }
	const identity = "0846919e53ac493dab3608a4ce409be7"
	for _, test := range []struct {
		job  scheduler.Job
		want string
	}{
		{scheduler.Job{State: "succeeded", Workdir: dir("/home/user/.molcrafts/molq/jobs/" + identity)}, identity},
		{scheduler.Job{State: "failed", Workdir: dir(archive + "/" + identity)}, identity},
		{scheduler.Job{State: "running", Workdir: dir("/home/user/.molcrafts/molq/jobs/" + identity)}, ""},
		{scheduler.Job{State: "failed", Workdir: dir("/home/user/.molcrafts/molq/jobs/project")}, ""},
		{scheduler.Job{State: "failed", Workdir: dir("/home/user/.molcrafts/molq/jobs")}, ""},
		{scheduler.Job{State: "failed", Workdir: dir("/home/user/" + identity)}, ""},
		{scheduler.Job{State: "failed"}, ""},
	} {
		if got := collectable(test.job, archive); got != test.want {
			t.Fatalf("collectable(%+v) = %q, want %q", test.job, got, test.want)
		}
	}
}

func waitTerminal(t *testing.T, service *Service, ref map[string]any) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		result, err := service.Call("jobs.get", map[string]any{"ref": ref})
		if err != nil {
			t.Fatal(err)
		}
		snapshots := result.(map[string]any)["snapshots"].([]map[string]any)
		if snapshots[0]["terminal"] == true {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("job did not finish")
}
