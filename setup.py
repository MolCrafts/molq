"""Build the serve binary into the Python package.

The wheel holds a static Go executable and no Python extension, so it is tagged
``py3-none-<platform>``. GOOS/GOARCH select a cross-compiled target, and
``--plat-name`` (passed verbatim, so compressed tag sets survive) names it.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from setuptools import setup
from setuptools.command.bdist_wheel import bdist_wheel
from setuptools.command.build_py import build_py
from setuptools.dist import Distribution

ROOT = Path(__file__).resolve().parent
BINARIES = ("molq", "molq.exe")


def binary_name() -> str:
    """Name the executable for the target, which may differ from the host."""
    target = os.environ.get("GOOS") or ("windows" if os.name == "nt" else "")
    return "molq.exe" if target == "windows" else "molq"


def build_binary() -> None:
    """Compile the target binary. Users do not need a Go toolchain."""
    destination = ROOT / "src" / "molq" / "_bin"
    destination.mkdir(parents=True, exist_ok=True)
    name = binary_name()
    for stale in BINARIES:
        if stale != name:
            (destination / stale).unlink(missing_ok=True)
    env = os.environ.copy()
    env["CGO_ENABLED"] = "0"
    subprocess.check_call(
        ["go", "build", "-trimpath", "-o", str(destination / name), "./cmd/molq"],
        cwd=ROOT,
        env=env,
    )


class BuildPy(build_py):
    """Place the serve executable in package data before copying sources."""

    def run(self) -> None:
        build_binary()
        super().run()


class BdistWheel(bdist_wheel):
    """Tag the wheel by platform only; any Python 3 can run the executable."""

    def finalize_options(self) -> None:
        super().finalize_options()
        self.root_is_pure = False

    def get_tag(self) -> tuple[str, str, str]:
        platform = self.plat_name if self.plat_name_supplied else super().get_tag()[2]
        return "py3", "none", platform


class BinaryDistribution(Distribution):
    """Mark the wheel as platform-specific because it contains the serve binary."""

    def has_ext_modules(self) -> bool:
        return True


setup(
    cmdclass={"build_py": BuildPy, "bdist_wheel": BdistWheel},
    distclass=BinaryDistribution,
)
