"""Tests for molq.options — SchedulerOptions types."""

import dataclasses

import pytest

from molq.options import (
    OPTIONS_TYPE_MAP,
    LocalSchedulerOptions,
    LSFSchedulerOptions,
    PBSSchedulerOptions,
    SlurmSchedulerOptions,
)


class TestLocalSchedulerOptions:
    def test_instantiates(self):
        opts = LocalSchedulerOptions()
        assert isinstance(opts, LocalSchedulerOptions)

    def test_frozen(self):
        opts = LocalSchedulerOptions()
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            opts.anything = "value"  # type: ignore[attr-defined]


class TestSlurmSchedulerOptions:
    def test_defaults(self):
        opts = SlurmSchedulerOptions()
        assert opts.sbatch_path == "sbatch"
        assert opts.squeue_path == "squeue"
        assert opts.scancel_path == "scancel"
        assert opts.sacct_path == "sacct"
        assert opts.extra_sbatch_flags == ()

    def test_custom_paths(self):
        opts = SlurmSchedulerOptions(
            sbatch_path="/opt/slurm/bin/sbatch",
            extra_sbatch_flags=("--export=ALL",),
        )
        assert opts.sbatch_path == "/opt/slurm/bin/sbatch"
        assert opts.extra_sbatch_flags == ("--export=ALL",)


class TestPBSSchedulerOptions:
    def test_defaults(self):
        opts = PBSSchedulerOptions()
        assert opts.qsub_path == "qsub"
        assert opts.qstat_path == "qstat"
        assert opts.qdel_path == "qdel"


class TestLSFSchedulerOptions:
    def test_defaults(self):
        opts = LSFSchedulerOptions()
        assert opts.bsub_path == "bsub"
        assert opts.bjobs_path == "bjobs"
        assert opts.bkill_path == "bkill"


class TestOptionsTypeMap:
    def test_all_schedulers_mapped(self):
        assert set(OPTIONS_TYPE_MAP.keys()) == {"local", "slurm", "pbs", "lsf"}

    def test_correct_types(self):
        assert OPTIONS_TYPE_MAP["local"] is LocalSchedulerOptions
        assert OPTIONS_TYPE_MAP["slurm"] is SlurmSchedulerOptions
        assert OPTIONS_TYPE_MAP["pbs"] is PBSSchedulerOptions
        assert OPTIONS_TYPE_MAP["lsf"] is LSFSchedulerOptions
