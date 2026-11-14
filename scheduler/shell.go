package scheduler

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/transport"
)

// Shell is a detached Bash allocation. Truth is the target receipt, or its
// collected copy in archive once the job directory left the target.
type Shell struct {
	core
	archive string
	// root overrides the target's JobRoot; only archive reads set it.
	root string
}

// NewShell binds one Shell destination.
func NewShell(target transport.Transport, cluster map[string]any) *Shell {
	return &Shell{core: core{transport: target, cluster: cluster}}
}

func (s *Shell) Validate(spec map[string]any) error {
	if nonempty(spec["resources"]) || nonempty(spec["scheduling"]) {
		return unsupported("Shell targets do not provide formal allocation resources or scheduling")
	}
	return s.validate(spec)
}

func (s *Shell) Render(spec map[string]any, directory string) (string, error) {
	body, err := RenderExecution(spec, s.launch)
	if err != nil {
		return "", err
	}
	d := quote(path.Join(directory, ControlDirectory))
	return `#!/usr/bin/env bash
umask 077
receipt_dir=` + d + `
start=$(LC_ALL=C ps -p $$ -o lstart=)
printf '%s\n%s\n' "$$" "$start" > "$receipt_dir/launch.tmp"
mv "$receipt_dir/launch.tmp" "$receipt_dir/launch"
finish() {
  code=$?
  trap - EXIT
  printf '%s\n' "$code" > "$receipt_dir/exit.tmp"
  mv "$receipt_dir/exit.tmp" "$receipt_dir/exit"
}
trap finish EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
cd -- "${receipt_dir%/*}" || exit 126
` + body, nil
}

func (s *Shell) Submit(ctx context.Context, spec map[string]any, directory string) (Job, error) {
	if err := s.prepareControl(ctx, directory, nil, ControlFiles); err != nil {
		return Job{}, err
	}
	scriptPath, err := s.stage(ctx, spec, directory, s.Validate, s.Render)
	if err != nil {
		return Job{}, err
	}
	script := "set -m; nohup bash " + quote(scriptPath) + " </dev/null >" + quote(controlPath(directory, "stdout")) + " 2>" + quote(controlPath(directory, "stderr")) + " &"
	if _, err := s.native(ctx, []string{"bash", "-c", script}, true, nil, false); err != nil {
		return Job{}, err
	}
	identity := path.Base(directory)
	for range 40 {
		jobs, err := s.QueryMany(ctx, []string{identity})
		if err != nil {
			return Job{}, protocol.FailOutcome("OUTCOME_UNKNOWN", "Shell accepted launch but evidence query failed", "unknown", map[string]any{"native_id": identity})
		}
		if job, ok := jobs[identity]; ok {
			return job, nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return Job{}, protocol.FailOutcome("OUTCOME_UNKNOWN", "Shell launch evidence unavailable", "unknown", map[string]any{"native_id": identity})
}

// rootWord is the job root as one target shell word, expanded by the target.
func (s *Shell) rootWord() string {
	if s.root != "" {
		return quote(s.root)
	}
	return `"$HOME"/` + quote(JobRoot)
}

// directory is a job directory as one target shell word.
func (s *Shell) directory(identity string) string {
	return s.rootWord() + "/" + quote(identity)
}

func (s *Shell) QueryMany(ctx context.Context, ids []string) (map[string]Job, error) {
	jobs, err := s.receipts(ctx, ids)
	if err != nil || s.archive == "" {
		return jobs, err
	}
	var collected []string
	for _, identity := range ids {
		if job, ok := jobs[identity]; (ok && job.State != "unknown") || identity == "." || identity == ".." || path.Base(identity) != identity {
			continue
		}
		if info, err := os.Stat(filepath.Join(s.archive, identity)); err == nil && info.IsDir() {
			collected = append(collected, identity)
		}
	}
	if len(collected) == 0 {
		return jobs, nil
	}
	// Only directories with an exit receipt are collected, so the local copy
	// answers without probing a target process.
	local := NewShell(transport.Local{}, s.cluster)
	local.root = filepath.ToSlash(s.archive)
	archived, err := local.receipts(ctx, collected)
	if err != nil {
		return nil, err
	}
	for identity, job := range archived {
		jobs[identity] = job
	}
	return jobs, nil
}

func (s *Shell) receipts(ctx context.Context, ids []string) (map[string]Job, error) {
	if len(ids) == 0 {
		return map[string]Job{}, nil
	}
	var commands []string
	for _, identity := range ids {
		commands = append(commands, `d=`+s.directory(identity)+`; work=$d; if [ -d "$d/.molq-control" ]; then d="$d/.molq-control"; fi; if [ -f "$d/launch" ]; then
read -r pid < "$d/launch"
start=$(sed -n '2p' "$d/launch")
state=unknown; code=''
if [ -f "$d/exit" ]; then
  read -r code < "$d/exit"
  state=failed; [ "$code" = 0 ] && state=succeeded
  if [ -f "$d/cancel_requested" ] && { [ "$code" = 143 ] || [ "$code" = 130 ]; }; then state=cancelled; fi
elif [ "$(LC_ALL=C ps -p "$pid" -o lstart= 2>/dev/null)" = "$start" ]; then state=running
fi
printf '%s|%s|%s|%s|%s|%s\n' `+quote(identity)+` "$pid" "$start" "$state" "$code" "$work"
fi`)
	}
	text, err := s.native(ctx, []string{"bash", "-c", strings.Join(commands, "\n")}, false, nil, false)
	if err != nil {
		return nil, err
	}
	jobs := map[string]Job{}
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "|", 6)
		if len(parts) != 6 || !strings.HasPrefix(parts[5], "/") {
			return nil, protocol.Fail("SCHEDULER_UNAVAILABLE", "Invalid Shell launch receipt", nil)
		}
		identity, pid, start, state, code := parts[0], parts[1], parts[2], parts[3], parts[4]
		if _, err := strconv.Atoi(pid); err != nil || strings.TrimSpace(start) == "" {
			return nil, protocol.Fail("SCHEDULER_UNAVAILABLE", "Invalid Shell launch receipt", nil)
		}
		var exitCode *int
		if code != "" {
			number, err := strconv.Atoi(code)
			if err != nil {
				return nil, protocol.Fail("SCHEDULER_UNAVAILABLE", "Invalid Shell launch receipt", nil)
			}
			exitCode = &number
		}
		dir := parts[5]
		jobs[identity] = Job{
			NativeID: identity, Incarnation: pid + ":" + start, State: state, Source: "shell_receipt",
			RawState: state, ExitCode: exitCode, Workdir: &dir,
		}
	}
	return jobs, nil
}

func (s *Shell) List(ctx context.Context) ([]Job, error) {
	text, err := s.native(ctx, []string{"bash", "-c", `for d in ` + s.rootWord() + `/*; do c=$d; [ ! -d "$d/.molq-control" ] || c="$d/.molq-control"; [ -f "$c/launch" ] && [ ! -f "$c/exit" ] && basename "$d"; done; true`}, false, nil, false)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		if line != "" {
			ids = append(ids, line)
		}
	}
	if len(ids) > 1024 {
		return nil, protocol.Fail("PARTIAL_FAILURE", "Shell receipt scope exceeds 1024; query explicit refs", nil)
	}
	jobs, err := s.QueryMany(ctx, ids)
	if err != nil {
		return nil, err
	}
	out := make([]Job, 0, len(jobs))
	for _, id := range ids {
		if job, ok := jobs[id]; ok {
			out = append(out, job)
		}
	}
	return out, nil
}

func (s *Shell) History(ctx context.Context, ids []string) (map[string]Job, error) {
	return s.QueryMany(ctx, ids)
}

func (s *Shell) HistoryRange(context.Context, time.Time, time.Time) ([]Job, error) {
	return nil, unsupported("This Scheduler does not support historical range queries")
}

func (s *Shell) Cancel(ctx context.Context, job Job) (map[string]any, error) {
	if Terminal(job.State) {
		return map[string]any{"outcome": "already_terminal"}, nil
	}
	script := `d=` + s.directory(job.NativeID) + `
[ ! -d "$d/.molq-control" ] || d="$d/.molq-control"
[ -f "$d/launch" ] || exit 2
read -r pid < "$d/launch"
start=$(sed -n '2p' "$d/launch")
[ "$(LC_ALL=C ps -p "$pid" -o lstart= 2>/dev/null)" = "$start" ] || exit 3
printf 'requested\n' > "$d/cancel_requested"
kill -TERM -- -"$pid"
`
	_, err := s.native(ctx, []string{"bash", "-c", script}, true, nil, false)
	if err != nil {
		if item, ok := err.(*protocol.Error); ok && item.Kind == "SUBMISSION_REJECTED" {
			return nil, protocol.Fail("CANCEL_REJECTED", "Target cancellation was rejected", item.Context)
		}
		return nil, err
	}
	return map[string]any{"outcome": "accepted"}, nil
}
