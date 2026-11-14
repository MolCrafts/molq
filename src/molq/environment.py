"""Build argv for execution in already prepared environments."""

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass

__all__ = [
    "ApptainerEnvironment",
    "ApptainerMount",
    "Environment",
    "CondaEnvironment",
    "UvEnvironment",
    "DockerEnvironment",
    "DockerMount",
]


class Environment(ABC):
    """A reusable, side-effect-free command wrapper."""

    @abstractmethod
    def wrap(self, argv: Sequence[str]) -> list[str]:
        """Wrap a literal command for execution on the target."""


def _command(argv: Sequence[str]) -> list[str]:
    if isinstance(argv, str) or not argv or not argv[0] or argv[0].startswith("-"):
        raise ValueError("Expected argv with a non-option executable")
    return list(argv)


@dataclass(frozen=True)
class CondaEnvironment(Environment):
    """An existing Conda environment, selected by name or prefix."""

    name: str | None = None
    prefix: str | None = None
    executable: str = "conda"

    def wrap(self, argv: Sequence[str]) -> list[str]:
        """Run with live stdout and stderr in the selected environment."""
        if bool(self.name) == bool(self.prefix):
            raise ValueError("Supply exactly one Conda environment name or prefix")
        selection = f"--name={self.name}" if self.name else f"--prefix={self.prefix}"
        return [
            self.executable,
            "run",
            "--no-capture-output",
            selection,
            "--",
            *_command(argv),
        ]


@dataclass(frozen=True)
class UvEnvironment(Environment):
    """An existing uv project environment on the execution host."""

    project: str
    executable: str = "uv"

    def wrap(self, argv: Sequence[str]) -> list[str]:
        """Use the prepared project without syncing or downloading Python."""
        if not self.project:
            raise ValueError("Supply the uv project directory")
        return [
            self.executable,
            "run",
            "--no-sync",
            "--no-python-downloads",
            "--offline",
            "--project",
            self.project,
            "--",
            *_command(argv),
        ]


@dataclass(frozen=True)
class DockerMount:
    """Bind an absolute Docker-host path to an absolute container path."""

    source: str
    target: str
    read_only: bool = False


@dataclass(frozen=True)
class DockerEnvironment(Environment):
    """An image already available to the target Docker daemon.

    Supply container variables as (name, value) pairs. The host environment
    is not implicitly forwarded.
    """

    image: str
    mounts: tuple[DockerMount, ...] = ()
    workdir: str | None = None
    env: tuple[tuple[str, str], ...] = ()
    executable: str = "docker"

    def wrap(self, argv: Sequence[str]) -> list[str]:
        """Run the literal executable as the container entrypoint."""
        command, *args = _command(argv)
        if not self.image or self.image.startswith("-"):
            raise ValueError("Supply a Docker image reference")
        wrapped = [self.executable, "run", "--rm", "--pull=never"]
        for mount in self.mounts:
            for path in (mount.source, mount.target):
                if not path.startswith("/") or any(char in path for char in ',"\r\n'):
                    raise ValueError(
                        "Docker bind paths must be absolute without commas or quotes"
                    )
            binding = f"type=bind,source={mount.source},target={mount.target}"
            if mount.read_only:
                binding += ",readonly"
            wrapped.extend(("--mount", binding))
        if self.workdir is not None:
            if not self.workdir.startswith("/"):
                raise ValueError("Docker workdir must be absolute inside the container")
            wrapped.extend(("--workdir", self.workdir))
        for key, value in self.env:
            if not key or "=" in key or "\x00" in key:
                raise ValueError("Invalid Docker environment variable name")
            wrapped.extend(("--env", f"{key}={value}"))
        return [*wrapped, "--entrypoint", command, self.image, *args]


@dataclass(frozen=True)
class ApptainerMount:
    """Bind a host path to a container path.

    Args:
        source: Absolute path on the execution host.
        target: Absolute path inside the container.
        read_only: Request a read-only bind.
    """

    source: str
    target: str
    read_only: bool = False


@dataclass(frozen=True)
class ApptainerEnvironment(Environment):
    """Run a command in a prepared SIF on the execution host.

    Args:
        image: Absolute path to an existing target-side `.sif` file.
        mounts: Explicit host-to-container binds.
        workdir: Optional absolute working directory inside the container.
        executable: Apptainer executable name or target path.
    """

    image: str
    mounts: tuple[ApptainerMount, ...] = ()
    workdir: str | None = None
    executable: str = "apptainer"

    def wrap(self, argv: Sequence[str]) -> list[str]:
        """Build literal Apptainer argv for the supplied command.

        Raises:
            ValueError: A command, image path, bind, or workdir is invalid.
        """
        if (
            not self.image.startswith("/")
            or not self.image.endswith(".sif")
            or "://" in self.image
            or any(char in self.image for char in "\x00\r\n")
        ):
            raise ValueError("Supply an absolute target path to a SIF (.sif) file")
        wrapped = [self.executable, "exec"]
        for mount in self.mounts:
            for path in (mount.source, mount.target):
                if not path.startswith("/") or any(
                    char in path for char in ",:\x00\r\n"
                ):
                    raise ValueError(
                        "Apptainer bind paths must be absolute without commas or colons"
                    )
            binding = f"{mount.source}:{mount.target}"
            if mount.read_only:
                binding += ":ro"
            wrapped.extend(("--bind", binding))
        if self.workdir is not None:
            if not self.workdir.startswith("/") or any(
                char in self.workdir for char in "\x00\r\n"
            ):
                raise ValueError(
                    "Apptainer workdir must be absolute inside the container"
                )
            wrapped.extend(("--pwd", self.workdir))
        return [*wrapped, "--", self.image, *_command(argv)]
