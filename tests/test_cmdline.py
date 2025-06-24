import subprocess
from typing import Generator

from molq.base import cmdline


class TestCMDLine:
    def test_cmdline(self):
        @cmdline
        def worker(
            second: int,
        ) -> Generator[dict, subprocess.CompletedProcess, subprocess.CompletedProcess]:
            print(f"start work {second}s")
            result = yield {
                "cmd": [f"echo", f"{second}"],
                "block": True,
            }
            print(result)
            return result

        assert worker(3).stdout.decode().strip() == "3"
