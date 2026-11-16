"""Register an explicit SSH computing destination; OpenSSH owns the alias."""

import json

from molq import submit

destination = submit(
    "research",
    "slurm",
    transport={"kind": "ssh", "alias": "hpc"},
    launcher="srun",
)
try:
    print(json.dumps(destination.definition(), ensure_ascii=False))
finally:
    destination.close()
