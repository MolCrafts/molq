"""Tests for molq.merge — defaults merge logic."""

from molq.merge import merge_defaults
from molq.models import SubmitorDefaults
from molq.types import Duration, JobExecution, JobResources, JobScheduling, Memory


class TestMergeDefaults:
    def test_no_defaults_no_override(self):
        r, s, e = merge_defaults(None)
        assert r == JobResources()
        assert s == JobScheduling()
        assert e == JobExecution()

    def test_defaults_only(self):
        defaults = SubmitorDefaults(
            resources=JobResources(cpu_count=4, memory=Memory.gb(8)),
            scheduling=JobScheduling(queue="normal"),
        )
        r, s, e = merge_defaults(defaults)
        assert r.cpu_count == 4
        assert r.memory == Memory.gb(8)
        assert s.queue == "normal"

    def test_override_only(self):
        r, s, e = merge_defaults(
            None,
            resources=JobResources(cpu_count=8),
            scheduling=JobScheduling(queue="gpu"),
        )
        assert r.cpu_count == 8
        assert s.queue == "gpu"

    def test_override_replaces_default(self):
        defaults = SubmitorDefaults(
            resources=JobResources(cpu_count=4, memory=Memory.gb(8)),
        )
        r, s, e = merge_defaults(
            defaults,
            resources=JobResources(memory=Memory.gb(32)),
        )
        # cpu_count=4 from defaults, memory=32GB from override
        assert r.cpu_count == 4
        assert r.memory == Memory.gb(32)

    def test_none_field_falls_back(self):
        defaults = SubmitorDefaults(
            resources=JobResources(cpu_count=4, memory=Memory.gb(8)),
        )
        # Override provides memory=None (default), so falls back to defaults
        r, _, _ = merge_defaults(
            defaults,
            resources=JobResources(gpu_count=2),
        )
        assert r.cpu_count == 4
        assert r.memory == Memory.gb(8)
        assert r.gpu_count == 2

    def test_no_deep_merge(self):
        """Dict fields are not merged — the override dict wins entirely."""
        defaults = SubmitorDefaults(
            execution=JobExecution(env={"A": "1", "B": "2"}),
        )
        _, _, e = merge_defaults(
            defaults,
            execution=JobExecution(env={"C": "3"}),
        )
        # Override dict replaces entirely, no merge of A/B keys
        assert e.env == {"C": "3"}

    def test_immutability(self):
        """Merge does not modify input objects."""
        defaults = SubmitorDefaults(
            resources=JobResources(cpu_count=4),
        )
        override = JobResources(memory=Memory.gb(16))

        r, _, _ = merge_defaults(defaults, resources=override)

        # Original objects unchanged
        assert defaults.resources.cpu_count == 4
        assert defaults.resources.memory is None
        assert override.cpu_count is None
        assert override.memory == Memory.gb(16)

        # Merged result has both
        assert r.cpu_count == 4
        assert r.memory == Memory.gb(16)

    def test_scheduling_merge(self):
        defaults = SubmitorDefaults(
            scheduling=JobScheduling(queue="normal", account="team-ml"),
        )
        _, s, _ = merge_defaults(
            defaults,
            scheduling=JobScheduling(queue="gpu"),
        )
        assert s.queue == "gpu"
        assert s.account == "team-ml"

    def test_execution_merge(self):
        defaults = SubmitorDefaults(
            execution=JobExecution(job_name="default_job", cwd="/work"),
        )
        _, _, e = merge_defaults(
            defaults,
            execution=JobExecution(job_name="my_job"),
        )
        assert e.job_name == "my_job"
        assert e.cwd == "/work"
