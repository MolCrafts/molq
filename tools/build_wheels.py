"""Cross-build every release wheel and prove each one carries the right binary.

The serve executable is static Go (CGO disabled), so one host builds all
targets. Each wheel is tagged py3-none-<platform>; a static Linux binary runs
on both glibc and musl, so Linux wheels carry a compressed manylinux/musllinux
tag. Go 1.26 needs macOS 12 and Windows 10.

Usage: python tools/build_wheels.py [--out DIST] [--sdist]
"""

from __future__ import annotations

import argparse
import os
import shutil
import struct
import subprocess
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Target:
    goos: str
    goarch: str
    platform: str


TARGETS = (
    Target("linux", "amd64", "manylinux2014_x86_64.musllinux_1_2_x86_64"),
    Target("linux", "arm64", "manylinux2014_aarch64.musllinux_1_2_aarch64"),
    Target("darwin", "amd64", "macosx_12_0_x86_64"),
    Target("darwin", "arm64", "macosx_12_0_arm64"),
    Target("windows", "amd64", "win_amd64"),
    Target("windows", "arm64", "win_arm64"),
)

# (format, machine) expected in the executable header for each GOARCH.
ELF_MACHINE = {"amd64": 62, "arm64": 183}
MACHO_CPU = {"amd64": 0x01000007, "arm64": 0x0100000C}
PE_MACHINE = {"amd64": 0x8664, "arm64": 0xAA64}


def describe(data: bytes) -> tuple[str, int]:
    """Return the executable format and machine id from its header."""
    if data[:4] == b"\x7fELF":
        return "linux", struct.unpack_from("<H", data, 18)[0]
    if data[:4] == b"\xcf\xfa\xed\xfe":
        return "darwin", struct.unpack_from("<I", data, 4)[0]
    if data[:2] == b"MZ":
        offset = struct.unpack_from("<I", data, 0x3C)[0]
        if data[offset : offset + 4] == b"PE\0\0":
            return "windows", struct.unpack_from("<H", data, offset + 4)[0]
    raise ValueError("unrecognized executable header")


def expected_machine(target: Target) -> int:
    table = {"linux": ELF_MACHINE, "darwin": MACHO_CPU, "windows": PE_MACHINE}
    return table[target.goos][target.goarch]


def verify(wheel: Path, target: Target) -> None:
    """Fail unless the wheel has the intended tag and exactly one matching binary."""
    tag = f"-py3-none-{target.platform}.whl"
    if not wheel.name.endswith(tag):
        raise SystemExit(f"{wheel.name}: expected tag suffix {tag}")
    name = "molq/_bin/molq.exe" if target.goos == "windows" else "molq/_bin/molq"
    with zipfile.ZipFile(wheel) as archive:
        binaries = [n for n in archive.namelist() if n.startswith("molq/_bin/")]
        if binaries != [name]:
            raise SystemExit(f"{wheel.name}: binaries {binaries}, expected [{name}]")
        info = archive.getinfo(name)
        header = archive.read(name)[:4096]
    goos, machine = describe(header)
    if (goos, machine) != (target.goos, expected_machine(target)):
        raise SystemExit(f"{wheel.name}: binary is {goos}/{machine:#x}")
    if target.goos != "windows" and not (info.external_attr >> 16) & 0o111:
        raise SystemExit(f"{wheel.name}: {name} is not executable")


def clean() -> None:
    shutil.rmtree(ROOT / "build", ignore_errors=True)
    for name in ("molq", "molq.exe"):
        (ROOT / "src" / "molq" / "_bin" / name).unlink(missing_ok=True)


def build(target: Target, out: Path) -> Path:
    clean()
    env = {**os.environ, "GOOS": target.goos, "GOARCH": target.goarch}
    before = set(out.glob("*.whl"))
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "build",
            "--wheel",
            "--outdir",
            str(out),
            f"--config-setting=--build-option=--plat-name={target.platform}",
        ],
        cwd=ROOT,
        env=env,
    )
    (wheel,) = set(out.glob("*.whl")) - before
    verify(wheel, target)
    return wheel


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=ROOT / "dist")
    parser.add_argument("--sdist", action="store_true", help="also build the sdist")
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    for stale in out.glob("molcrafts_molq-*"):
        stale.unlink()
    try:
        if args.sdist:
            clean()
            subprocess.check_call(
                [sys.executable, "-m", "build", "--sdist", "--outdir", str(out)],
                cwd=ROOT,
            )
        for target in TARGETS:
            print(f"built {build(target, out).name}")
    finally:
        clean()


if __name__ == "__main__":
    main()
