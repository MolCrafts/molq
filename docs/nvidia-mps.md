# GPUs and NVIDIA MPS

Request accelerators as allocation resources. Your target program owns GPU use.

## GPU allocation

```python
from molq import Accelerator, Resources

gpu_resources = Resources(
    nodes=1,
    tasks=1,
    cpus_per_task=4,
    accelerators=(Accelerator(kind="gpu", quantity=1, scope="per_node"),),
)
```

Slurm supports per-node or total GPU requests. OpenPBS supports per-node GPUs.
A model selector requires an exact native mapping; see [scheduler limits](schedulers.md#scheduler-mappings).

## MPS allocation

```python
mps_resources = Resources(
    nodes=1,
    tasks=1,
    cpus_per_task=2,
    accelerators=(Accelerator(kind="nvidia_mps", quantity=20, scope="per_node"),),
)
```

Slurm renders this as `--gres=mps:20`. The quantity uses the site's configured
MPS resource units. The scheduler must advertise that resource.

molq does not start an MPS daemon or replace a rejected MPS request with a whole
GPU allocation. Put any site-required MPS setup and cleanup in your payload.
The [MPS example](https://github.com/MolCrafts/molq/blob/master/examples/10_nvidia_mps.py)
shows an explicit payload.
