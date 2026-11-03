package serve

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"
)

const (
	oldJob   = "0846919e53ac493dab3608a4ce409be7"
	freshJob = "1f2e3d4c5b6a79881f2e3d4c5b6a7988"
	liveJob  = "aaaaaaaabbbbbbbbccccccccdddddddd"
)

// jobTree lays out a job root with one directory per retention case. Old means
// 40 days without change; the default window is 30.
func jobTree(t *testing.T, root string) {
	t.Helper()
	old := time.Now().Add(-40 * 24 * time.Hour)
	write := func(dir, name string, at time.Time) {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o700); err != nil {
			t.Fatal(err)
		}
		file := filepath.Join(root, dir, name)
		if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(file, at, at); err != nil {
			t.Fatal(err)
		}
	}
	write(oldJob, "stdout", old)
	write(freshJob, "stdout", time.Now())
	// A running job keeps writing its log in an otherwise old directory.
	write(liveJob, "job.sh", old)
	write(liveJob, "stdout", time.Now())
	// Old names that are not a job identity are never touched.
	write("notes", "keep", old)
	write("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", "keep", old)
	write("0846919e53ac493dab3608a4ce409be", "keep", old)
	write(".0846919e53ac493dab3608a4ce409be8.partial", "keep", old)
	if err := os.WriteFile(filepath.Join(root, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.Mkdir(outside, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "cccccccccccccccccccccccccccccccc")); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() && entry.Name() != freshJob && entry.Name() != liveJob {
			if err := os.Chtimes(filepath.Join(root, entry.Name()), old, old); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := os.Chtimes(filepath.Join(root, liveJob), old, old); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(outside, old, old); err != nil {
		t.Fatal(err)
	}
}

// assertPurged checks that only oldJob left root.
func assertPurged(t *testing.T, root string) {
	t.Helper()
	if _, err := os.Lstat(filepath.Join(root, oldJob)); !os.IsNotExist(err) {
		t.Fatalf("old job directory kept: %v", err)
	}
	for _, name := range []string{
		freshJob, liveJob, "notes", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", "0846919e53ac493dab3608a4ce409be",
		".0846919e53ac493dab3608a4ce409be8.partial", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "cccccccccccccccccccccccccccccccc",
	} {
		if _, err := os.Lstat(filepath.Join(root, name)); err != nil {
			t.Fatalf("%s removed: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "cccccccccccccccccccccccccccccccc")); err != nil {
		t.Fatalf("symlink target removed: %v", err)
	}
}

func purgeService(t *testing.T) (*Service, string) {
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
	t.Setenv("HOME", filepath.Join(root, "home"))
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

func purgeResults(t *testing.T, service *Service, params map[string]any) (map[string]any, []map[string]any) {
	t.Helper()
	result, err := service.Call("jobs.purge", params)
	if err != nil {
		t.Fatal(err)
	}
	report := result.(map[string]any)
	return report, report["results"].([]map[string]any)
}

func TestPurgeDeletesOnlyOldJobDirectoriesOnLocalTarget(t *testing.T) {
	service, root := purgeService(t)
	id := register(t, service, map[string]any{"name": "local", "scheduler": "shell"})
	target := filepath.Join(root, "home", ".molcrafts", "molq", "jobs")
	jobTree(t, target)

	report, results := purgeResults(t, service, map[string]any{})
	assertPurged(t, target)
	if report["retention_days"] != 30 || report["partial_failure"] != false || len(results) != 1 {
		t.Fatalf("report = %#v", report)
	}
	if results[0]["cluster_id"] != id || results[0]["location"] != "target" || !slices.Equal(results[0]["removed"].([]string), []string{oldJob}) {
		t.Fatalf("result = %#v", results[0])
	}
}

func TestPurgeDeletesOldArchivedAndSSHTargetDirectories(t *testing.T) {
	loopbackSSH(t)
	service, root := purgeService(t)
	id := register(t, service, map[string]any{
		"name": "remote", "scheduler": "shell", "transport": map[string]any{"kind": "ssh", "alias": "fixture"},
	})
	target := filepath.Join(root, "home", ".molcrafts", "molq", "jobs")
	archive := filepath.Join(root, "collected", id)
	// A Cluster removed since still leaves its archive behind.
	orphan := filepath.Join(root, "collected", "0123456789abcdef0123456789abcdef")
	for _, dir := range []string{target, archive, orphan} {
		jobTree(t, dir)
	}

	report, results := purgeResults(t, service, map[string]any{})
	for _, dir := range []string{target, archive, orphan} {
		assertPurged(t, dir)
	}
	if report["partial_failure"] != false || len(results) != 3 {
		t.Fatalf("report = %#v", report)
	}
	for _, result := range results {
		if !slices.Equal(result["removed"].([]string), []string{oldJob}) {
			t.Fatalf("result = %#v", result)
		}
	}
}

func TestPurgeSkipsUnreachableClusterAndContinues(t *testing.T) {
	loopbackSSH(t)
	service, root := purgeService(t)
	broken := register(t, service, map[string]any{
		"name": "broken", "scheduler": "shell", "transport": map[string]any{"kind": "ssh", "alias": "unreachable"},
	})
	register(t, service, map[string]any{"name": "local", "scheduler": "shell"})
	target := filepath.Join(root, "home", ".molcrafts", "molq", "jobs")
	archive := filepath.Join(root, "collected", broken)
	jobTree(t, target)
	jobTree(t, archive)

	report, results := purgeResults(t, service, map[string]any{})
	assertPurged(t, target)
	assertPurged(t, archive)
	if report["partial_failure"] != true || len(results) != 3 {
		t.Fatalf("report = %#v", report)
	}
	failed := 0
	for _, result := range results {
		if result["error"] != nil {
			failed++
			if result["cluster_id"] != broken || result["location"] != "target" {
				t.Fatalf("failure attributed to %#v", result)
			}
		}
	}
	if failed != 1 {
		t.Fatalf("failures = %d in %#v", failed, results)
	}
	// Serving goes on after the failure.
	if _, err := service.Call("clusters.list", map[string]any{}); err != nil {
		t.Fatal(err)
	}
}

func TestPurgeOneClusterAndUnknownCluster(t *testing.T) {
	service, root := purgeService(t)
	id := register(t, service, map[string]any{"name": "local", "scheduler": "shell"})
	jobTree(t, filepath.Join(root, "home", ".molcrafts", "molq", "jobs"))
	_, results := purgeResults(t, service, map[string]any{"cluster": "local"})
	if len(results) != 1 || results[0]["cluster_id"] != id {
		t.Fatalf("results = %#v", results)
	}
	if _, err := service.Call("jobs.purge", map[string]any{"cluster": "missing"}); err == nil {
		t.Fatal("expected unknown cluster failure")
	}
}

func TestPurgeDisabledAtZeroRetention(t *testing.T) {
	service, root := purgeService(t)
	if err := service.UseConfig(filepath.Join(root, "config.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Call("config.set", map[string]any{"key": "job_retention_days", "value": 0}); err != nil {
		t.Fatal(err)
	}
	register(t, service, map[string]any{"name": "local", "scheduler": "shell"})
	target := filepath.Join(root, "home", ".molcrafts", "molq", "jobs")
	archive := filepath.Join(root, "collected", "0123456789abcdef0123456789abcdef")
	jobTree(t, target)
	jobTree(t, archive)

	report, results := purgeResults(t, service, map[string]any{})
	if report["retention_days"] != 0 || len(results) != 0 {
		t.Fatalf("report = %#v", report)
	}
	for _, dir := range []string{target, archive} {
		if _, err := os.Stat(filepath.Join(dir, oldJob)); err != nil {
			t.Fatalf("disabled purge removed %s: %v", dir, err)
		}
	}
}

func TestPurgeHonoursConfiguredWindow(t *testing.T) {
	service, root := purgeService(t)
	if err := service.UseConfig(filepath.Join(root, "config.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Call("config.set", map[string]any{"key": "job_retention_days", "value": 50}); err != nil {
		t.Fatal(err)
	}
	register(t, service, map[string]any{"name": "local", "scheduler": "shell"})
	target := filepath.Join(root, "home", ".molcrafts", "molq", "jobs")
	jobTree(t, target)
	_, results := purgeResults(t, service, map[string]any{})
	if len(results[0]["removed"].([]string)) != 0 {
		t.Fatalf("40-day directory purged under a 50-day window: %#v", results)
	}
	if _, err := os.Stat(filepath.Join(target, oldJob)); err != nil {
		t.Fatal(err)
	}
}

func TestRetainJobDirectoriesPurgesAtStart(t *testing.T) {
	service, root := purgeService(t)
	register(t, service, map[string]any{"name": "local", "scheduler": "shell"})
	target := filepath.Join(root, "home", ".molcrafts", "molq", "jobs")
	jobTree(t, target)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		service.RetainJobDirectories(ctx, time.Hour)
		close(done)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Lstat(filepath.Join(target, oldJob)); os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("start-up purge did not run")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	assertPurged(t, target)
}
