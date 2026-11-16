package scheduler

import (
	"context"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/transport"
)

// Shell runs a detached Bash allocation and reads its original project receipts.
type Shell struct{ core }

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
	return `#!/usr/bin/env bash
exec 3<&-
umask 077
receipt_dir=` + quote(directory) + `
start=$(LC_ALL=C ps -p $$ -o lstart=) && [ -n "$start" ] && machine=$(uname -n) && [ -n "$machine" ] || exit 126
finish() {
  code=$?
  trap - EXIT
  printf '%s\n%s\n' "$code" "${termination:-exit}" > "$receipt_dir/exit.tmp" &&
    mv -- "$receipt_dir/exit.tmp" "$receipt_dir/exit"
}
trap finish EXIT
trap 'termination=signal; exit 143' TERM
trap 'termination=signal; exit 130' INT
printf '%s\n%s\n%s\n' "$$" "$start" "$machine" > "$receipt_dir/launch.tmp" &&
  mv -- "$receipt_dir/launch.tmp" "$receipt_dir/launch" || exit 126
cd -- "$receipt_dir" || exit 126
` + body, nil
}

func (s *Shell) Submit(ctx context.Context, spec map[string]any, directory string) (Job, error) {
	script, err := s.Render(spec, directory)
	if err != nil {
		return Job{}, err
	}
	if err := s.prepareRun(ctx, directory, nil); err != nil {
		return Job{}, err
	}
	// Bash job control gives the detached allocation its own process group.
	// The child opens and unlinks the staging file before interpreting it.
	launch := stageScript + `
exec 4>"$2/stdout" 5>"$2/stderr" || exit 1
set -m
nohup bash -c 'exec 3<"$1" || exit 126
rm -- "$1" || exit 126
exec bash /dev/fd/3' bash "$script" </dev/null >&4 2>&5 4>&- 5>&- &
trap - EXIT
`
	if _, err := s.native(ctx, []string{"bash", "-c", launch, "bash", itoa(len(script)), directory}, true, []byte(script), true); err != nil {
		return Job{}, err
	}
	project, identity := RunLocation(directory)
	for range 40 {
		jobs, err := s.QueryMany(ctx, []string{identity}, project)
		if err != nil {
			return Job{}, submissionUnknown(identity)
		}
		if job, ok := jobs[identity]; ok {
			return job, nil
		}
		select {
		case <-ctx.Done():
			return Job{}, submissionUnknown(identity)
		case <-time.After(25 * time.Millisecond):
		}
	}
	return Job{}, submissionUnknown(identity)
}

const shellProjectPath = `project=${root%/.molq/jobs}
[ "$(cd -P -- "$project" 2>/dev/null && pwd -P)" = "$project" ] &&
  [ ! -L "${root%/*}" ] && [ ! -L "$root" ]`

const shellRunPath = `d="$root/$id"
[ ! -L "${root%/*}" ] && [ ! -L "$root" ] && [ ! -L "$d" ] && [ -d "$d" ]`

const shellLaunchReceipt = `[ ! -L "$d/launch" ] && [ -f "$d/launch" ] || return 1
{ IFS= read -r pid && IFS= read -r start && IFS= read -r machine; } < "$d/launch" || return 1
case "$pid" in ''|*[!0-9]*) return 1;; esac
[ "$pid" -gt 1 ] && [ -n "$start" ] && [ -n "$machine" ]`

func (s *Shell) QueryMany(ctx context.Context, ids []string, projectRoot string) (map[string]Job, error) {
	jobs := make(map[string]Job, len(ids))
	if len(ids) == 0 {
		return jobs, nil
	}
	if projectRoot == "" {
		return nil, protocol.Fail("INVALID_INPUT", "Shell query requires a project root", nil)
	}
	args := []string{"sh", "-c", `root=$1; shift
` + shellProjectPath + ` || exit 0
current_machine=$(uname -n) || exit 1
read_launch() {
` + shellLaunchReceipt + `
}
for id do
  ` + shellRunPath + ` || continue
  read_launch || continue
  code=; state=unknown
  if [ ! -L "$d/exit" ] && [ -f "$d/exit" ]; then
    { IFS= read -r code && IFS= read -r termination; } < "$d/exit" || continue
    case "$termination:$code" in
      exit:0) state=succeeded;;
      exit:*) state=failed;;
      signal:130|signal:143) state=cancelled;;
      *) continue;;
    esac
  elif [ "$machine" = "$current_machine" ] && [ "$(LC_ALL=C ps -p "$pid" -o lstart= 2>/dev/null)" = "$start" ]; then
    state=running
  fi
  printf '%s|%s|%s|%s|%s|%s\n' "$id" "$pid" "$start" "$machine" "$state" "$code"
done
`, "sh", path.Join(projectRoot, ProjectJobs)}
	for _, id := range ids {
		if !runIdentity.MatchString(id) {
			return nil, protocol.Fail("INVALID_INPUT", "Invalid Shell run identity", nil)
		}
		args = append(args, id)
	}
	output, err := s.native(ctx, args, false, nil, false)
	if err != nil {
		return nil, err
	}
	for _, line := range strings.Split(strings.TrimSuffix(output, "\n"), "\n") {
		if line == "" {
			continue
		}
		parts := strings.Split(line, "|")
		if len(parts) != 6 {
			return nil, protocol.Fail("SCHEDULER_UNAVAILABLE", "Invalid Shell receipt", nil)
		}
		id, pid, start, machine, state, code := parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]
		var exitCode *int
		if code != "" {
			value, err := strconv.Atoi(code)
			if err != nil || value < 0 || value > 255 {
				return nil, protocol.Fail("SCHEDULER_UNAVAILABLE", "Invalid Shell exit receipt", nil)
			}
			exitCode = &value
		}
		dir := path.Join(projectRoot, ProjectJobs, id)
		job := Job{
			NativeID: id, Incarnation: pid + ":" + start + ":" + machine, State: state, Source: "shell_receipt",
			RawState: state, ExitCode: exitCode, Workdir: &dir, ProjectRoot: projectRoot, RunID: id,
			StdoutPath: strPtr(path.Join(dir, "stdout")), StderrPath: strPtr(path.Join(dir, "stderr")),
		}
		if state == "cancelled" {
			job.FailureReason = "cancelled"
		}
		jobs[id] = job
	}
	return jobs, nil
}

func (s *Shell) List(ctx context.Context, projectRoot string) ([]Job, error) {
	if projectRoot == "" {
		return nil, protocol.Fail("INVALID_INPUT", "Shell listing requires project_root", nil)
	}
	output, err := s.native(ctx, []string{"sh", "-c", `root=$1
` + shellProjectPath + ` || exit 1
count=0
for d in "$root"/*; do
  [ ! -L "$d" ] && [ -d "$d" ] && [ -f "$d/launch" ] && [ ! -e "$d/exit" ] || continue
  printf '%s\n' "${d##*/}"
  count=$((count+1))
  [ "$count" -le 1024 ] || break
done`, "sh", path.Join(projectRoot, ProjectJobs)}, false, nil, false)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, id := range strings.Fields(output) {
		if runIdentity.MatchString(id) {
			ids = append(ids, id)
		}
	}
	if len(ids) > 1024 {
		return nil, protocol.Fail("PARTIAL_FAILURE", "Shell active receipt scope exceeds 1024; query explicit refs", nil)
	}
	jobs, err := s.QueryMany(ctx, ids, projectRoot)
	if err != nil {
		return nil, err
	}
	result := make([]Job, 0, len(jobs))
	for _, id := range ids {
		if job, ok := jobs[id]; ok {
			result = append(result, job)
		}
	}
	return result, nil
}

func (s *Shell) History(ctx context.Context, ids []string) (map[string]Job, error) {
	return nil, unsupported("Shell history requires project-qualified refs")
}

func (s *Shell) HistoryRange(context.Context, time.Time, time.Time) ([]Job, error) {
	return nil, unsupported("This Scheduler does not support historical range queries")
}

func (s *Shell) Cancel(ctx context.Context, job Job) (map[string]any, error) {
	if Terminal(job.State) {
		return map[string]any{"outcome": "already_terminal"}, nil
	}
	if job.ProjectRoot == "" || !runIdentity.MatchString(job.NativeID) {
		return nil, protocol.Fail("JOB_IDENTITY_UNVERIFIABLE", "Shell run location unavailable", nil)
	}
	script := `root=$1; id=$2
` + shellProjectPath + ` || exit 2
` + shellRunPath + ` || exit 2
[ ! -e "$d/exit" ] || exit 2
read_launch() {
` + shellLaunchReceipt + `
}
read_launch && [ "$pid:$start:$machine" = "$3" ] && [ "$(uname -n)" = "$machine" ] || exit 2
[ "$(LC_ALL=C ps -p "$pid" -o lstart= 2>/dev/null)" = "$start" ] || exit 3
[ "$(LC_ALL=C ps -p "$pid" -o pgid= 2>/dev/null | tr -d '[:space:]')" = "$pid" ] || exit 3
kill -TERM -- -"$pid"
`
	_, err := s.native(ctx, []string{"bash", "-c", script, "bash", path.Join(job.ProjectRoot, ProjectJobs), job.NativeID, job.Incarnation}, true, nil, false)
	if err != nil {
		if item, ok := err.(*protocol.Error); ok && item.Kind == "SUBMISSION_REJECTED" {
			return nil, protocol.Fail("CANCEL_REJECTED", "Target cancellation was rejected", item.Context)
		}
		return nil, err
	}
	return map[string]any{"outcome": "accepted"}, nil
}
