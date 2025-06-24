"""Public interface for job submission.

This module exposes the :class:`submit` decorator used to register and submit
jobs to different compute backends.
"""

from .base import YieldDecorator
from .submitor.base import BaseSubmitor


def get_submitor(cluster_name: str, cluster_type: str) -> BaseSubmitor:
    """
    Get the submitor for the given cluster name and type.

    Args:
        cluster_name (str): identify name of the cluster
        cluster_type (str): type of the cluster, e.g. slurm, local

    Raises:
        ValueError: if the cluster type is not supported

    Returns:
        BaseSubmitor: submitor class
    """
    if cluster_type == "slurm":
        from .submitor.slurm import SlurmSubmitor

        return SlurmSubmitor(cluster_name)
    elif cluster_type == "local":
        from .submitor.local import LocalSubmitor

        return LocalSubmitor(cluster_name)
    else:
        raise ValueError(f"Cluster type {cluster_type} not supported.")


class submit(YieldDecorator):
    """Decorator that sends generator-based tasks to a registered submitter."""

    CLUSTERS: dict[str, BaseSubmitor] = dict()

    def __new__(
        cls,
        cluster_name: str,
        cluster_type: str | None = None,
    ):
        """Create or reuse a submitter bound to ``cluster_name``."""
        if cluster_name not in cls.CLUSTERS:
            cls.CLUSTERS[cluster_name] = get_submitor(cluster_name, cluster_type)
        return super().__new__(cls)

    def __init__(
        self,
        cluster_name: str,
        cluster_type: str | None = None,
    ):
        """Store reference to the submitter created in :py:meth:`__new__`."""
        self._current_submitor = submit.CLUSTERS[cluster_name]

    def validate_yield(self, yield_result):
        """Defer validation to the underlying submitter."""
        return yield_result

    def after_yield(self, yield_result):
        """Submit the job using the current submitter."""
        return self._current_submitor.submit(yield_result)

    # ------------------------------------------------------------------
    # helper APIs
    # ------------------------------------------------------------------

    @classmethod
    def get_n_clusters(cls) -> int:
        """Return number of registered clusters."""
        return len([k for k in cls.CLUSTERS.keys() if not k.startswith("_")])

    @classmethod
    def get_cluster(cls, name: str) -> BaseSubmitor:
        """Return the submitor instance for ``name``."""
        return cls.CLUSTERS[name]
