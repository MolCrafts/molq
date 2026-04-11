# CLI Reference

Molq provides a Typer + Rich CLI for managing jobs from the terminal.

## Global

```bash
molq --help
```

## Commands

### `molq submit`

Submit a job to a scheduler backend.

```bash
molq submit SCHEDULER [COMMAND]... [OPTIONS]
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `SCHEDULER` | Backend: `local`, `slurm`, `pbs`, `lsf` |
| `COMMAND` | Command and arguments to execute |

**Options:**

| Option | Description |
|--------|-------------|
| `--cpus INT` | CPU cores |
| `--mem TEXT` | Memory (e.g. `8G`, `512M`) |
| `--time TEXT` | Time limit (e.g. `4h`, `01:30:00`) |
| `--gpus INT` | GPU count |
| `--gpu-type TEXT` | GPU type (e.g. `a100`) |
| `--queue TEXT` | Queue/partition name |
| `--name TEXT` | Job name |
| `--workdir TEXT` | Working directory |
| `--account TEXT` | Billing account |
| `--cluster TEXT` | Cluster namespace |
| `--block` | Wait for completion |

**Examples:**

```bash
# Local execution
molq submit local echo "Hello World"

# SLURM with resources
molq submit slurm --cpus 8 --mem 32G --time 4h python train.py

# GPU job
molq submit slurm --gpus 2 --gpu-type a100 --mem 64G python train.py

# Wait for completion
molq submit local --block python long_script.py
```

---

### `molq list`

List submitted jobs.

```bash
molq list [SCHEDULER] [OPTIONS]
```

**Arguments:**

| Argument | Default | Description |
|----------|---------|-------------|
| `SCHEDULER` | `local` | Scheduler backend |

**Options:**

| Option | Description |
|--------|-------------|
| `--cluster TEXT` | Cluster namespace |
| `--all` | Include terminal (finished) jobs |

**Example:**

```bash
molq list slurm --all --cluster hpc
```

---

### `molq status`

Get the status of a specific job.

```bash
molq status JOB_ID [SCHEDULER]
```

**Arguments:**

| Argument | Default | Description |
|----------|---------|-------------|
| `JOB_ID` | (required) | Job UUID |
| `SCHEDULER` | `local` | Scheduler backend |

**Options:**

| Option | Description |
|--------|-------------|
| `--cluster TEXT` | Cluster namespace |

---

### `molq watch`

Watch a job until it reaches a terminal state.

```bash
molq watch JOB_ID [SCHEDULER] [OPTIONS]
```

**Arguments:**

| Argument | Default | Description |
|----------|---------|-------------|
| `JOB_ID` | (required) | Job UUID |
| `SCHEDULER` | `local` | Scheduler backend |

**Options:**

| Option | Description |
|--------|-------------|
| `--cluster TEXT` | Cluster namespace |
| `--timeout FLOAT` | Maximum wait time in seconds |

---

### `molq cancel`

Cancel a running or pending job.

```bash
molq cancel JOB_ID [SCHEDULER]
```

**Arguments:**

| Argument | Default | Description |
|----------|---------|-------------|
| `JOB_ID` | (required) | Job UUID |
| `SCHEDULER` | `local` | Scheduler backend |

**Options:**

| Option | Description |
|--------|-------------|
| `--cluster TEXT` | Cluster namespace |
