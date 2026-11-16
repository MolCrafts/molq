"""Submit native MPS or a user-managed MPS payload to a registered Slurm cluster.

Run: python examples/10_nvidia_mps.py native|manual
The target must have train.py and its dependencies in the configured work directory.
Resource quantities below are examples; choose your own allocation budget.
"""

import argparse
import json

from molq import Accelerator, Execution, ExecutionUnit, JobSpec, Resources, submit

MPS_SCRIPT = 'set -euo pipefail\nmps_root=$(mktemp -d "${TMPDIR:-/tmp}/molq-mps.XXXXXX")\nexport CUDA_MPS_PIPE_DIRECTORY="$mps_root/pipe"\nexport CUDA_MPS_LOG_DIRECTORY="$mps_root/log"\nmkdir -p "$CUDA_MPS_PIPE_DIRECTORY" "$CUDA_MPS_LOG_DIRECTORY"\ncleanup() {\n    printf \'quit\\n\' | nvidia-cuda-mps-control || true\n    rm -rf "$mps_root"\n}\ntrap cleanup EXIT\nnvidia-cuda-mps-control -d\npython train.py\n'


def request(mode: str) -> JobSpec:
    """Construct either a native MPS request or an ordinary GPU allocation."""
    unit = (
        ExecutionUnit.argv("main", ["python", "train.py"])
        if mode == "native"
        else ExecutionUnit.script(
            "main", MPS_SCRIPT, env={"CUDA_MPS_ACTIVE_THREAD_PERCENTAGE": "50"}
        )
    )
    return JobSpec(
        execution=Execution((unit,), cwd="/scratch/user/project"),
        resources=Resources(
            nodes=1,
            tasks=1,
            cpus_per_task=4,
            memory_bytes="8589934592",
            time_limit_seconds=3600,
            accelerators=(
                Accelerator("nvidia_mps", 20, "per_node")
                if mode == "native"
                else Accelerator("gpu", 1, "per_node"),
            ),
        ),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("native", "manual"))
    parser.add_argument("--project-root", required=True)
    args = parser.parse_args()
    hpc = submit("hpc", project_root=args.project_root)

    @hpc
    def train(mode):
        job = yield request(mode)
        return job

    try:
        job = train(args.mode)
        print(json.dumps(job.wait(), ensure_ascii=False))
        print(json.dumps(job.logs(), ensure_ascii=False))
    finally:
        hpc.close()
