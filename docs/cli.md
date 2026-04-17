# CLI Reference

`molq` ships a Typer + Rich command-line interface for submission, inspection, monitoring, cleanup, and background reconciliation.

## Global Help

```bash
molq --help
```

## `molq submit`

Submit a new job.

```bash
molq submit SCHEDULER [COMMAND]... [OPTIONS]
```

Arguments:

- `SCHEDULER` — one of `local`, `slurm`, `pbs`, `lsf`
- `COMMAND` — command and arguments to execute

**Resource options:**

| Option | Description |
|---|---|
| `--cpus` | CPU core count |
| `--mem` | Memory, e.g. `8G`, `512M` |
| `--time` | Time limit, e.g. `4h`, `2h30m`, `04:00:00` |
| `--gpus` | GPU count |
| `--gpu-type` | GPU model string passed to the scheduler |

**Scheduling options:**

| Option | Description |
|---|---|
| `--queue` | Queue / partition name |
| `--account` | Billing account |

**Execution options:**

| Option | Description |
|---|---|
| `--name` | Job name |
| `--workdir` | Working directory |

**Retry options:**

| Option | Description |
|---|---|
| `--retries N` | Maximum total attempts (1 = no retry) |
| `--retry-on-exit-code CODE` | Retry only when the job exits with this code (repeatable) |

**Dependency options** (SLURM, PBS, LSF only — accepts Molq job IDs, not scheduler IDs):

| Option | Condition | Runs after upstream… |
|---|---|---|
| `--after-success JOB_ID` | `after_success` | succeeds |
| `--after-failure JOB_ID` | `after_failure` | fails / is cancelled / times out |
| `--after-started JOB_ID` | `after_started` | begins executing |
| `--after JOB_ID` | `after` | reaches any terminal state |

All four dependency options are repeatable. Multiple job IDs with the same condition may be provided by repeating the flag.

**Other options:**

| Option | Description |
|---|---|
| `--cluster` | Override cluster namespace |
| `--profile` | Load a named profile from `~/.molq/config.toml` |
| `--config` | Path to a custom config file |
| `--block` | Wait for the job to reach a terminal state before returning |

Examples:

```bash
# Basic submission
molq submit local echo "hello"
molq submit slurm --cpus 8 --mem 32G --time 4h python train.py
molq submit slurm --gpus 2 --queue gpu --block python train.py

# Retries
molq submit slurm --retries 3 python train.py
molq submit slurm --retries 3 --retry-on-exit-code 1 python train.py

# Dependencies
JOB1=$(molq submit slurm python preprocess.py | awk '/ID:/{print $2}')

molq submit slurm --after-success $JOB1  python train.py
molq submit slurm --after-failure $JOB1  python notify_failure.py
molq submit slurm --after-started $JOB1  python sidecar_monitor.py
molq submit slurm --after         $JOB1  python cleanup.py

# Multiple upstreams (repeat the flag)
molq submit slurm \
  --after-success $JOB1 \
  --after-success $JOB2 \
  python merge_results.py

# Profile-based submission
molq submit slurm --profile gpu python train.py
```

## `molq list`

List jobs for a scheduler and cluster namespace.

```bash
molq list [SCHEDULER] [OPTIONS]
```

Options:

- `--cluster`
- `--profile`
- `--config`
- `--all`

## `molq status`

Show the current persisted state for a job.

```bash
molq status JOB_ID [SCHEDULER] [OPTIONS]
```

Options:

- `--cluster`
- `--profile`
- `--config`

## `molq watch`

Block until a job reaches a terminal state.

```bash
molq watch JOB_ID [SCHEDULER] [OPTIONS]
```

Options:

- `--cluster`
- `--profile`
- `--config`
- `--timeout`

Behavior:

- if retries are enabled, `watch` follows the latest attempt in the retry family before returning the final terminal record.

## `molq logs`

Print captured stdout or stderr.

By default, these artifact files are created under the submission working
directory at `.molq/jobs/<job-id>/` unless you override `jobs_dir`,
`output_file`, or `error_file`.

```bash
molq logs JOB_ID [SCHEDULER] [OPTIONS]
```

Options:

- `--cluster`
- `--profile`
- `--config`
- `--stream stdout|stderr|both`
- `--tail`
- `--follow`

Behavior:

- `--follow` tails artifact files until the job reaches terminal state and the file reaches EOF.
- `--stream both` prefixes output lines with `[stdout]` or `[stderr]`.

## `molq history`

Show persisted job history for the current scheduler and cluster namespace.

```bash
molq history [SCHEDULER] [OPTIONS]
```

Options:

- `--cluster`
- `--profile`
- `--config`
- `--all`

Behavior:

- history includes attempt number so retry families can be scanned without opening SQLite directly.

## `molq inspect`

Show canonical metadata, runtime paths, transitions, retry lineage, and dependency graph for a single job.

Output includes:

- All persisted fields (`state`, `scheduler_job_id`, stdout/stderr paths, etc.)
- Retry family — all attempts for the same root job
- Upstream dependencies with satisfaction state (`✓` satisfied, `·` pending, `!` impossible)
- Downstream dependents (jobs waiting on this job)

```bash
molq inspect JOB_ID [SCHEDULER] [OPTIONS]
```

Options:

- `--cluster`
- `--profile`
- `--config`

## `molq cancel`

Cancel the latest active attempt for a job family.

```bash
molq cancel JOB_ID [SCHEDULER] [OPTIONS]
```

Options:

- `--cluster`
- `--profile`
- `--config`

## `molq cleanup`

Prune retained job directories and old terminal records.

```bash
molq cleanup [SCHEDULER] [OPTIONS]
```

Options:

- `--cluster`
- `--profile`
- `--config`
- `--dry-run`

## `molq daemon`

Run the lightweight background reconciliation loop.

```bash
molq daemon [SCHEDULER] [OPTIONS]
```

Options:

- `--cluster`
- `--profile`
- `--config`
- `--once`
- `--interval`
- `--skip-cleanup`

## `molq monitor`

Open the full-screen dashboard across all clusters in the configured database.

```bash
molq monitor [OPTIONS]
```

Options:

- `--all`, `-a` — include terminal jobs
- `--limit`, `-n` — limit the number of displayed rows
- `--refresh`, `-r` — refresh interval in seconds
- `--db` — path to a specific SQLite database
