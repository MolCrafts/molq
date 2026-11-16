"""Resolve named outputs and selectively copy them over the existing rsync RPC."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from stat import S_ISDIR, S_ISREG
from types import MappingProxyType

from molq.client.path import Path

__all__ = [
    "ArtifactManifest",
    "Catalog",
    "Glob",
    "OutputPath",
    "ResolvedArtifacts",
    "ManifestDelivery",
]

_MAX_CATALOG_BYTES = 1024 * 1024
_SAFE_LONG_OPTIONS = frozenset(
    {
        "--archive",
        "--recursive",
        "--compress",
        "--checksum",
        "--times",
        "--perms",
        "--verbose",
        "--quiet",
        "--partial",
        "--itemize-changes",
    }
)


@dataclass(frozen=True)
class OutputPath:
    """Select one literal relative path."""

    path: str
    required: bool = True


@dataclass(frozen=True)
class Glob:
    """Select regular files matching a relative glob."""

    pattern: str
    required: bool = True


@dataclass(frozen=True)
class Catalog:
    """Resolve a named group from a task-produced JSON catalog."""

    path: str
    key: str
    required: bool = True


def _relative(value: object) -> str:
    if (
        not isinstance(value, str)
        or any(part in ("", ".", "..") for part in value.split("/"))
        or any(char in value for char in "\x00\r\n\\")
    ):
        raise ValueError(f"Expected a relative artifact path: {value!r}")
    return value


def _unique_object(items: list[tuple[str, object]]) -> dict:
    result = {}
    for name, value in items:
        if name in result:
            raise ValueError(f"Duplicate catalog key: {name!r}")
        result[name] = value
    return result


def _catalog(source: Path) -> dict:
    if not S_ISREG(source.stat().st_mode):
        raise ValueError(f"Expected a catalog file: {source}")
    body = source.read_range(0, _MAX_CATALOG_BYTES + 1)
    if len(body) > _MAX_CATALOG_BYTES:
        raise ValueError("Artifact catalog exceeds 1 MiB")
    data = json.loads(body, object_pairs_hook=_unique_object)
    if not isinstance(data, dict):
        raise ValueError("Artifact catalog must be a JSON object")
    return data


@dataclass(frozen=True)
class ManifestDelivery:
    """Relative paths delivered under the destination, in transfer order."""

    location: Path
    paths: tuple[str, ...]
    transfer_results: tuple[dict, ...]


@dataclass(frozen=True)
class ResolvedArtifacts:
    """A fixed selection of relative paths under one output root."""

    source: Path
    paths: tuple[str, ...]
    names: Mapping[str, tuple[str, ...]]
    _directories: frozenset[str]

    def download(
        self, destination: Path, *, options: Sequence[str] = (), timeout: int = 90
    ) -> ManifestDelivery:
        """Copy selected paths, preserving relative layout and RPC errors."""
        options = tuple(options)
        for option in options:
            if option not in _SAFE_LONG_OPTIONS and not (
                option.startswith("-")
                and len(option) > 1
                and set(option[1:]) <= set("arzctpviq")
            ):
                raise ValueError(f"Unsupported manifest rsync option: {option!r}")
        if self._directories and not any(
            option in {"--archive", "--recursive"}
            or (not option.startswith("--") and ("a" in option or "r" in option))
            for option in options
        ):
            raise ValueError(
                "Directory downloads require an explicit rsync recursion option"
            )

        # Use the source connection for all destination operations as well.
        destination = Path(destination, molq=self.source._molq or destination._molq)
        if not destination.is_absolute() or ".." in destination.parts:
            raise ValueError("Expected an absolute artifact destination root")
        if destination.cluster is None and destination._molq is not None:
            if destination._molq._wire.endpoint:
                raise ValueError(
                    "Use a local-transport Cluster path for a separate serve host"
                )
        destination = destination.resolve()
        target_root = Path(destination, root=destination.as_posix())
        for relative in self.paths:
            mode = (self.source / relative).stat().st_mode
            if not (S_ISDIR(mode) if relative in self._directories else S_ISREG(mode)):
                raise ValueError(
                    f"Artifact type changed after resolution: {relative!r}"
                )
        results = []
        for relative in self.paths:
            target = target_root / relative
            if relative not in self._directories and target.is_dir():
                raise IsADirectoryError(str(target))
            target.parent.mkdir(parents=True, exist_ok=True)
            results.append(
                (self.source / relative).transfer(
                    target,
                    *options,
                    timeout=timeout,
                    directory=relative in self._directories,
                )
            )
        return ManifestDelivery(destination, self.paths, tuple(results))


@dataclass(frozen=True)
class ArtifactManifest:
    """Named literal paths, globs and task-generated catalog groups."""

    outputs: Mapping[str, OutputPath | Glob | Catalog]

    def __post_init__(self) -> None:
        object.__setattr__(self, "outputs", MappingProxyType(dict(self.outputs)))
        for name, output in self.outputs.items():
            if "/" in _relative(name):
                raise ValueError(f"Output names must be one path component: {name!r}")
            _relative(output.pattern if isinstance(output, Glob) else output.path)

    def resolve(self, source: Path, *, select: Sequence[str]) -> ResolvedArtifacts:
        """Resolve only selected names and members without copying files."""
        if isinstance(select, str) or not select:
            raise ValueError("Select at least one named output")
        if not source.is_absolute() or ".." in source.parts:
            raise ValueError("Expected an absolute artifact source root")
        if (
            source.cluster is None
            and source._molq is not None
            and source._molq._wire.endpoint
        ):
            raise ValueError(
                "Use a local-transport Cluster path for a separate serve host"
            )
        source = source.resolve()
        if not source.is_dir():
            raise NotADirectoryError(str(source))
        root = Path(source, root=source.as_posix())
        catalogs: dict[str, dict] = {}
        names = {}
        paths: dict[str, bool] = {}
        for selection in select:
            name, separator, member = _relative(selection).partition("/")
            output = self.outputs[name]
            if separator and (not isinstance(output, Catalog) or "/" in member):
                raise ValueError(f"Invalid catalog selection: {selection!r}")
            try:
                if isinstance(output, Catalog):
                    if output.path not in catalogs:
                        catalogs[output.path] = _catalog(root / output.path)
                    group = catalogs[output.path][output.key]
                    if not isinstance(group, dict):
                        raise ValueError(
                            f"Expected a catalog group object: {output.key}"
                        )
                    candidates = [group[member]] if separator else list(group.values())
                elif isinstance(output, Glob):
                    candidates = sorted(
                        path.relative_to(root).as_posix()
                        for path in root.glob(output.pattern)
                    )
                else:
                    candidates = [output.path]
            except (FileNotFoundError, KeyError):
                if output.required:
                    raise
                candidates = []
            matched = []
            for candidate in candidates:
                relative = _relative(candidate)
                try:
                    mode = (root / relative).stat().st_mode
                except FileNotFoundError:
                    if output.required:
                        raise
                    continue
                if isinstance(output, Glob) and not S_ISREG(mode):
                    continue
                if not (S_ISREG(mode) or S_ISDIR(mode)):
                    raise ValueError(f"Expected a file or directory: {relative!r}")
                paths[relative] = S_ISDIR(mode)
                if relative not in matched:
                    matched.append(relative)
            if output.required and not matched:
                raise FileNotFoundError(f"No artifact paths for {selection!r}")
            names[selection] = tuple(matched)
        return ResolvedArtifacts(
            root,
            tuple(paths),
            MappingProxyType(names),
            frozenset(relative for relative, directory in paths.items() if directory),
        )
