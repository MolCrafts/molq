# CLI Reference

Molq provides a command-line interface (CLI) for submitting and managing jobs directly from the terminal. This is useful for running one-off scripts, checking job status, or integrating Molq into shell-based pipelines.

## Global Options

| Option | Description |
|--------|-------------|
| `--version` | Show the version and exit. |
| `--help` | Show the help message and exit. |

## Commands

### `molq submit`

Submits a job to a specified scheduler. You can pass the command as arguments or pipe a script via stdin.

**Usage:**
```bash
molq submit [OPTIONS] {local|slurm} [COMMAND_ARGS]...
```

**Options:**

| Option | Alias | Description |
|--------|-------|-------------|
| `--cpu-count` | `--n-cpus` | Number of CPU cores to request. |
| `--memory` | `--mem` | Memory requirement (e.g., `8G`, `512M`). |
| `--time` | `--time-limit` | Time limit (e.g., `2h`, `30m`, `1d`). |
| `--queue` | `--partition` | Queue or partition name. |
| `--gpu-count` | `--n-gpus` | Number of GPUs to request. |
| `--gpu-type` | | Specific GPU type (e.g., `V100`, `A100`). |
| `--job-name` | | Human-readable name for the job. |
| `--workdir` | | Working directory for execution. |
| `--email` | | Email address for notifications. |
| `--priority` | | Job priority (`low`, `normal`, `high`). |
| `--account` | | Account to charge resources to. |
| `--block` / `--no-block` | | Wait for job completion (default: `False`). |
| `--cleanup` / `--no-cleanup` | | Clean up temporary files (default: `True`). |
| `--dry-run` | | Show the job configuration without submitting. |

**Examples:**

Run a simple command locally:
```bash
molq submit local echo "Hello World"
```

Submit a job to SLURM with resource requirements:
```bash
molq submit slurm --cpu-count 8 --memory 16G srun lmp -in input.lmp
```

Submit a script from pipe:
```bash
echo "python train.py" | molq submit slurm --n-gpus 1 --time 4h
```

---

### `molq list`

Lists submitted jobs. By default, it shows active jobs.

**Usage:**
```bash
molq list [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--scheduler` | Filter by scheduler (`local` or `slurm`). |
| `--all-history` | Show all jobs including finished/cancelled ones. |
| `--verbose` | Show detailed job information. |

---

### `molq status`

Get the status of a specific job.

**Usage:**
```bash
molq status [OPTIONS] JOB_ID
```

**Options:**

| Option | Description |
|--------|-------------|
| `--scheduler` | Scheduler to query (`local` or `slurm`). If omitted, you must format `JOB_ID` as `scheduler-id`. |

**Example:**
```bash
molq status slurm-12345
# or
molq status 12345 --scheduler slurm
```

---

### `molq cancel`

Cancels a running or pending job.

**Usage:**
```bash
molq cancel [OPTIONS] JOB_ID
```

**Options:**

| Option | Description |
|--------|-------------|
| `--scheduler` | Scheduler execution the job (`local` or `slurm`). |

---

### `molq config`

Manage Molq configuration. This allows you to set defaults for your environment.

**Commands:**

#### `molq config show`
Show the current configuration.
```bash
molq config show [KEY]
```

#### `molq config set`
Set a configuration value.
```bash
molq config set KEY VALUE
```

**Example:**
Set default SLURM host:
```bash
molq config set slurm.host my-cluster-node
```
