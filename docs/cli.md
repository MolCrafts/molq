# CLI Reference

`molq` ships a Typer + Rich command-line interface for submission, inspection, and monitoring.

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

Key options:

- `--cpus`
- `--mem`
- `--time`
- `--gpus`
- `--gpu-type`
- `--queue`
- `--name`
- `--workdir`
- `--account`
- `--cluster`
- `--block`

Examples:

```bash
molq submit local echo "hello"
molq submit slurm --cpus 8 --mem 32G --time 4h python train.py
molq submit slurm --gpus 2 --queue gpu --block python train.py
```

## `molq list`

List jobs for a scheduler and cluster namespace.

```bash
molq list [SCHEDULER] [OPTIONS]
```

Options:

- `--cluster`
- `--all`

## `molq status`

Show the current persisted state for a job.

```bash
molq status JOB_ID [SCHEDULER] [OPTIONS]
```

Options:

- `--cluster`

## `molq watch`

Block until a job reaches a terminal state.

```bash
molq watch JOB_ID [SCHEDULER] [OPTIONS]
```

Options:

- `--cluster`
- `--timeout`

## `molq logs`

Print captured stdout or stderr.

```bash
molq logs JOB_ID [SCHEDULER] [OPTIONS]
```

Options:

- `--cluster`
- `--stream stdout|stderr`
- `--tail`

## `molq cancel`

Cancel a running job.

```bash
molq cancel JOB_ID [SCHEDULER] [OPTIONS]
```

Options:

- `--cluster`

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
