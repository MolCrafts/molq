"""Tests for molq.types — Memory, Duration, Script, resource specs."""

import dataclasses
from pathlib import Path

import pytest

from molq.types import (
    Duration,
    JobExecution,
    JobResources,
    JobScheduling,
    Memory,
    Script,
)

# ---------------------------------------------------------------------------
# Memory
# ---------------------------------------------------------------------------


class TestMemoryFactory:
    def test_kb(self):
        assert Memory.kb(1).bytes == 1024

    def test_mb(self):
        assert Memory.mb(1).bytes == 1024**2

    def test_gb(self):
        assert Memory.gb(8).bytes == 8 * 1024**3

    def test_tb(self):
        assert Memory.tb(1).bytes == 1024**4

    def test_float_input(self):
        assert Memory.gb(0.5).bytes == 1024**3 // 2


class TestMemoryParse:
    def test_parse_gb(self):
        assert Memory.parse("8GB").bytes == 8 * 1024**3

    def test_parse_mb(self):
        assert Memory.parse("512MB").bytes == 512 * 1024**2

    def test_parse_kb(self):
        assert Memory.parse("1024KB").bytes == 1024 * 1024

    def test_parse_plain_bytes(self):
        assert Memory.parse("1024").bytes == 1024

    def test_parse_case_insensitive(self):
        assert Memory.parse("8gb") == Memory.parse("8GB")

    def test_parse_with_spaces(self):
        assert Memory.parse("  8 GB  ").bytes == 8 * 1024**3

    def test_parse_float(self):
        assert Memory.parse("4.5GB").bytes == int(4.5 * 1024**3)

    def test_parse_empty_raises(self):
        with pytest.raises(ValueError, match="Empty"):
            Memory.parse("")

    def test_parse_invalid_raises(self):
        with pytest.raises(ValueError):
            Memory.parse("abc")

    def test_parse_unknown_unit_raises(self):
        with pytest.raises(ValueError, match="Unknown memory unit"):
            Memory.parse("8XB")


class TestMemoryFormat:
    def test_to_slurm_gb(self):
        assert Memory.gb(8).to_slurm() == "8G"

    def test_to_slurm_mb(self):
        assert Memory.mb(512).to_slurm() == "512M"

    def test_to_slurm_tb(self):
        assert Memory.tb(1).to_slurm() == "1T"

    def test_to_slurm_kb(self):
        assert Memory.kb(256).to_slurm() == "256K"

    def test_to_pbs_gb(self):
        assert Memory.gb(8).to_pbs() == "8gb"

    def test_to_pbs_mb(self):
        assert Memory.mb(512).to_pbs() == "512mb"

    def test_to_lsf_kb(self):
        assert Memory.gb(8).to_lsf_kb() == 8 * 1024**2


class TestMemoryComparison:
    def test_ordering(self):
        assert Memory.mb(512) < Memory.gb(1)
        assert Memory.gb(1) > Memory.mb(512)

    def test_equality(self):
        assert Memory.gb(1) == Memory.mb(1024)

    def test_frozen(self):
        m = Memory.gb(8)
        with pytest.raises(dataclasses.FrozenInstanceError):
            m.bytes = 0  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Duration
# ---------------------------------------------------------------------------


class TestDurationFactory:
    def test_minutes(self):
        assert Duration.minutes(30).seconds == 1800

    def test_hours(self):
        assert Duration.hours(2).seconds == 7200


class TestDurationParse:
    def test_parse_hms(self):
        assert Duration.parse("01:30:00").seconds == 5400

    def test_parse_ms(self):
        assert Duration.parse("30:00").seconds == 1800

    def test_parse_human_readable(self):
        assert Duration.parse("2h30m").seconds == 9000

    def test_parse_single_unit(self):
        assert Duration.parse("90m").seconds == 5400

    def test_parse_plain_seconds(self):
        assert Duration.parse("3600").seconds == 3600

    def test_parse_hms_hours_exceed_24(self):
        # 40:00:00 → 40 hours
        assert Duration.parse("40:00:00").seconds == 40 * 3600

    def test_parse_slurm_day_format(self):
        # 1-00:00:00 → 1 day = 86400s
        assert Duration.parse("1-00:00:00").seconds == 86400

    def test_parse_slurm_day_format_with_hours(self):
        # 2-06:30:00 → 2 days + 6h + 30m
        assert Duration.parse("2-06:30:00").seconds == 2 * 86400 + 6 * 3600 + 30 * 60

    def test_parse_empty_raises(self):
        with pytest.raises(ValueError, match="Empty"):
            Duration.parse("")

    def test_parse_invalid_raises(self):
        with pytest.raises(ValueError):
            Duration.parse("abc")


class TestDurationFormat:
    def test_to_slurm_hours(self):
        assert Duration.hours(2).to_slurm() == "02:00:00"

    def test_to_slurm_with_days(self):
        assert Duration.hours(25).to_slurm() == "1-01:00:00"

    def test_to_pbs(self):
        assert Duration.hours(2).to_pbs() == "02:00:00"

    def test_to_lsf_minutes(self):
        assert Duration.hours(2).to_lsf_minutes() == 120

    def test_to_lsf_minutes_rounds_up(self):
        # 61 seconds -> 2 minutes (rounded up)
        assert Duration(seconds=61).to_lsf_minutes() == 2


class TestDurationComparison:
    def test_ordering(self):
        assert Duration.minutes(30) < Duration.hours(1)

    def test_equality(self):
        assert Duration.minutes(60) == Duration.hours(1)

    def test_frozen(self):
        d = Duration.hours(1)
        with pytest.raises(dataclasses.FrozenInstanceError):
            d.seconds = 0  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Script
# ---------------------------------------------------------------------------


class TestScript:
    def test_inline(self):
        s = Script.inline("echo hello")
        assert s.variant == "inline"
        assert s.text == "echo hello"
        assert s.file_path is None

    def test_path(self, tmp_path: Path):
        f = tmp_path / "run.sh"
        f.write_text("#!/bin/bash\necho hi")
        s = Script.path(f)
        assert s.variant == "path"
        assert s.text is None
        assert s.file_path == f.resolve()

    def test_path_string(self, tmp_path: Path):
        f = tmp_path / "run.sh"
        f.write_text("#!/bin/bash")
        s = Script.path(str(f))
        assert s.file_path == f.resolve()

    def test_frozen(self):
        s = Script.inline("echo hi")
        with pytest.raises(dataclasses.FrozenInstanceError):
            s._variant = "x"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Resource specs
# ---------------------------------------------------------------------------


class TestJobResources:
    def test_defaults(self):
        r = JobResources()
        assert r.cpu_count is None
        assert r.memory is None
        assert r.gpu_count is None
        assert r.gpu_type is None
        assert r.time_limit is None

    def test_with_values(self):
        r = JobResources(
            cpu_count=8,
            memory=Memory.gb(32),
            gpu_count=2,
            gpu_type="A100",
            time_limit=Duration.hours(4),
        )
        assert r.cpu_count == 8
        assert r.memory == Memory.gb(32)

    def test_frozen(self):
        r = JobResources(cpu_count=4)
        with pytest.raises(dataclasses.FrozenInstanceError):
            r.cpu_count = 8  # type: ignore[misc]


class TestJobScheduling:
    def test_defaults(self):
        s = JobScheduling()
        assert s.queue is None
        assert s.exclusive_node is False

    def test_with_values(self):
        s = JobScheduling(queue="gpu", account="team-ml")
        assert s.queue == "gpu"
        assert s.account == "team-ml"


class TestJobExecution:
    def test_defaults(self):
        e = JobExecution()
        assert e.env is None
        assert e.cwd is None

    def test_with_values(self):
        e = JobExecution(
            env={"CUDA_VISIBLE_DEVICES": "0"},
            cwd="/work",
            job_name="train",
        )
        assert e.env == {"CUDA_VISIBLE_DEVICES": "0"}
        assert e.job_name == "train"
