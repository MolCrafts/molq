"""A caller-owned workspace on a registered destination."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import PurePosixPath

from molq.artifact_manifest import ArtifactManifest, ManifestDelivery
from molq.client.path import Path


@dataclass(frozen=True)
class Workspace:
    """Bind a cluster directory without creating or taking ownership of it.

    Args:
        root: An absolute, cluster-qualified path. An existing access scope
            continues to constrain workspace operations.
    """

    root: Path

    def __post_init__(self) -> None:
        if (
            not isinstance(self.root, Path)
            or not self.root.cluster
            or not self.root.is_absolute()
            or ".." in self.root.parts
            or any(char in self.root.as_posix() for char in "\x00\r\n")
        ):
            raise ValueError("Workspace requires an absolute cluster path")

    def ensure(self) -> Path:
        """Create the workspace if needed and return its scoped root."""
        # Check and create through the caller's original scope first.
        self.root.mkdir(parents=True, exist_ok=True)
        return self.path()

    def path(self, relative: str = ".") -> Path:
        """Return a child path constrained to this workspace.

        Args:
            relative: A relative POSIX path inside the workspace.

        Raises:
            ValueError: The path is absolute, cluster-qualified or traverses up.
        """
        if not isinstance(relative, str) or any(
            char in relative for char in ":\\\x00\r\n"
        ):
            raise ValueError("Expected a relative workspace path")
        child = PurePosixPath(relative)
        if not relative or child.is_absolute() or ".." in child.parts:
            raise ValueError("Expected a relative workspace path")
        # Preserve inherited scopes before narrowing to the workspace. Resolve
        # on the target so an alias cannot grant access outside the outer root.
        root = self.root
        if root._access_root is not None:
            root = root.resolve()
        return Path(root, root=root.as_posix()) / child

    def upload(
        self,
        source: Path,
        relative: str,
        *options: str,
        timeout: int = 90,
        directory: bool | None = None,
    ) -> dict:
        """Copy a source to an explicit existing-parent path in this workspace.

        Args:
            source: File or directory to copy.
            relative: Destination path relative to the workspace.
            *options: Explicit rsync options.
            timeout: Transfer limit in seconds.
            directory: Set when the source type is known or on a separate serve host.

        Returns:
            The Runtime transfer result.

        Raises:
            ValueError: The paths are bound to different clients.
        """
        target = self.path(relative)
        if (
            source._molq is not None
            and target._molq is not None
            and source._molq is not target._molq
        ):
            raise ValueError("Bind upload paths to the same Molq client")
        return Path(source, molq=target._molq).transfer(
            target, *options, timeout=timeout, directory=directory
        )

    def download(
        self,
        manifest: ArtifactManifest,
        destination: Path,
        *,
        select: Sequence[str],
        relative: str = ".",
        options: Sequence[str] = (),
        timeout: int = 90,
    ) -> ManifestDelivery:
        """Resolve output names under this workspace and copy the selection.

        Args:
            manifest: Named file, glob, or catalog declarations.
            destination: Explicit destination root.
            select: Output names or catalog members.
            relative: Output root relative to the workspace.
            options: Explicit rsync options.
            timeout: Limit in seconds for each transfer.

        Returns:
            The selected paths and their transfer results.
        """
        resolved = manifest.resolve(self.path(relative), select=select)
        return resolved.download(destination, options=options, timeout=timeout)
