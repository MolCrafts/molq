package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestSubmitEcho(t *testing.T) {
	root := t.TempDir()
	binary := filepath.Join(root, "molq")
	command := exec.Command("go", "build", "-o", binary, ".")
	command.Env = append(os.Environ(), "CGO_ENABLED=0")
	command.Dir = "."
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	registry := filepath.Join(root, "registry.db")
	definition := `{"name":"local","scheduler":"shell"}`
	register := exec.Command(binary, "--registry", registry, "cluster", "register", "--definition", definition)
	if output, err := register.CombinedOutput(); err != nil {
		t.Fatalf("register: %v\n%s", err, output)
	}
	submit := exec.Command(binary, "--registry", registry, "job", "submit", "local", "--", "echo", "cli")
	output, err := submit.CombinedOutput()
	if err != nil {
		t.Fatalf("submit: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), `"outcome":"accepted"`) && !strings.Contains(string(output), `"outcome": "accepted"`) {
		t.Fatalf("submit output = %s", output)
	}
}

func TestSlurmSubmitFromWorkspaceLeavesOnlyManagedLogs(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("local POSIX target execution")
	}
	binary, err := clientExecutable()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	home := filepath.Join(root, "target home")
	work := filepath.Join(home, "work")
	if err := os.MkdirAll(work, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("PATH", root+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("SBATCH_OUTPUT", filepath.Join(work, "unwanted.out"))
	for name, body := range map[string]string{
		"sbatch": `#!/bin/sh
set -eu
stdout=$SBATCH_OUTPUT
for arg do
  case "$arg" in
    --parsable|--job-name=*|--no-requeue) ;;
    --chdir=*) directory=${arg#*=} ;;
    --output=*) stdout=${arg#*=} ;;
    --error=*) stderr=${arg#*=} ;;
    *) exit 2 ;;
  esac
done
cd "$directory"
bash -s > "$stdout" 2> "$stderr"
printf '123\n'
`,
		"squeue": `#!/bin/sh
set -eu
set -- "$HOME"/.molcrafts/molq/jobs/*
printf '{"jobs":[{"job_id":"123","job_state":"RUNNING","submit_time":1710000000,"current_working_directory":"%s"}]}\n' "$1"
`,
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	registry := filepath.Join(root, "registry.db")
	register := exec.Command(binary, "--registry", registry, "cluster", "register", "--definition", `{"name":"hpc","scheduler":"slurm"}`)
	if output, err := register.CombinedOutput(); err != nil {
		t.Fatalf("register: %v\n%s", err, output)
	}
	submit := exec.Command(binary, "--registry", registry, "job", "submit", "hpc", "--spec", "@-")
	submit.Dir = work
	submit.Stdin = strings.NewReader(`{"execution":{"units":[{"id":"main","command":{"kind":"argv","argv":["pwd"]}}]}}`)
	if output, err := submit.CombinedOutput(); err != nil {
		t.Fatalf("submit: %v\n%s", err, output)
	}
	if entries, err := os.ReadDir(work); err != nil || len(entries) != 0 {
		t.Fatalf("workspace entries = %v, err = %v", entries, err)
	}
	jobRoot := filepath.Join(home, ".molcrafts", "molq", "jobs")
	jobs, err := os.ReadDir(jobRoot)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("job directories = %v, err = %v", jobs, err)
	}
	directory := filepath.Join(jobRoot, jobs[0].Name())
	if entries, err := os.ReadDir(directory); err != nil || len(entries) != 2 || entries[0].Name() != "stderr" || entries[1].Name() != "stdout" {
		t.Fatalf("job files = %v, err = %v", entries, err)
	}
	if stdout, err := os.ReadFile(filepath.Join(directory, "stdout")); err != nil || strings.TrimSpace(string(stdout)) != directory {
		t.Fatalf("job cwd = %q, err = %v", stdout, err)
	}
}

func TestTransferTakesRsyncSyntax(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("rsync is not installed")
	}
	root := t.TempDir()
	binary := filepath.Join(root, "molq")
	command := exec.Command("go", "build", "-o", binary, ".")
	command.Env = append(os.Environ(), "CGO_ENABLED=0")
	command.Dir = "."
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	registry := filepath.Join(root, "registry.db")
	source := filepath.Join(root, "source")
	destination := filepath.Join(root, "destination")
	if err := os.MkdirAll(source, 0o755); err != nil || os.MkdirAll(destination, 0o755) != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "note.txt"), []byte("copied"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"from", "to"} {
		definition := `{"name":"` + name + `","scheduler":"shell"}`
		register := exec.Command(binary, "--registry", registry, "cluster", "register", "--definition", definition)
		if output, err := register.CombinedOutput(); err != nil {
			t.Fatalf("register %s: %v\n%s", name, err, output)
		}
	}
	transfer := exec.Command(binary, "transfer", "-a", "--registry", registry, "--exclude", "skip.txt", "note.txt", "skip.txt", "to:"+filepath.Join(destination)+"/")
	transfer.Dir = source
	if err := os.WriteFile(filepath.Join(source, "skip.txt"), []byte("skipped"), 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := transfer.CombinedOutput()
	if err != nil {
		t.Fatalf("transfer: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), `"outcome":"applied"`) && !strings.Contains(string(output), `"outcome": "applied"`) {
		t.Fatalf("transfer output = %s", output)
	}
	body, err := os.ReadFile(filepath.Join(destination, "note.txt"))
	if err != nil || string(body) != "copied" {
		t.Fatalf("body = %q, %v", body, err)
	}
	if _, err := os.Stat(filepath.Join(destination, "skip.txt")); !os.IsNotExist(err) {
		t.Fatalf("excluded skip.txt err = %v", err)
	}
}

func TestPurgeKeepsUnverifiedOldJobDirectories(t *testing.T) {
	binary, err := clientExecutable()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	t.Setenv("HOME", filepath.Join(root, "home"))
	registry := filepath.Join(root, "registry.db")
	register := exec.Command(binary, "--registry", registry, "cluster", "register", "--definition", `{"name":"local","scheduler":"shell"}`)
	if output, err := register.CombinedOutput(); err != nil {
		t.Fatalf("register: %v\n%s", err, output)
	}
	const identity = "0846919e53ac493dab3608a4ce409be7"
	directory := filepath.Join(root, "home", ".molcrafts", "molq", "jobs", identity)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-40 * 24 * time.Hour)
	if err := os.Chtimes(directory, old, old); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command(binary, "--registry", registry, "job", "purge", "local").CombinedOutput()
	if err != nil {
		t.Fatalf("purge: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), `"removed":[]`) {
		t.Fatalf("purge output = %s", output)
	}
	if _, err := os.Stat(directory); err != nil {
		t.Fatalf("unverified job directory removed: %v", err)
	}
	if output, err := exec.Command(binary, "--registry", registry, "job", "purge", "a", "b").CombinedOutput(); err == nil {
		t.Fatalf("purge with two clusters succeeded: %s", output)
	}
}
