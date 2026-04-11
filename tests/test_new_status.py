"""Tests for molq.status — JobState enum."""

from molq.status import JobState


class TestJobState:
    def test_all_values_exist(self):
        expected = {
            "created",
            "submitted",
            "queued",
            "running",
            "succeeded",
            "failed",
            "cancelled",
            "timed_out",
            "lost",
        }
        actual = {s.value for s in JobState}
        assert actual == expected

    def test_terminal_states(self):
        terminal = {s for s in JobState if s.is_terminal}
        assert terminal == {
            JobState.SUCCEEDED,
            JobState.FAILED,
            JobState.CANCELLED,
            JobState.TIMED_OUT,
            JobState.LOST,
        }

    def test_non_terminal_states(self):
        non_terminal = {s for s in JobState if not s.is_terminal}
        assert non_terminal == {
            JobState.CREATED,
            JobState.SUBMITTED,
            JobState.QUEUED,
            JobState.RUNNING,
        }

    def test_string_value(self):
        assert JobState.RUNNING == "running"
        assert JobState.TIMED_OUT == "timed_out"

    def test_from_string(self):
        assert JobState("succeeded") is JobState.SUCCEEDED
        assert JobState("lost") is JobState.LOST
