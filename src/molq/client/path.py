"""A POSIX path that also names the cluster holding it.

``Path("arrhenius:/scratch/run")`` reads like an rsync operand: a colon before
the first slash names a registered cluster, and a path with no cluster is on
this machine. Pure path arithmetic is inherited from ``PurePosixPath`` and
keeps the cluster. I/O on a local path is ``pathlib``; I/O on a cluster path is
public ``files.*`` RPC through a ``Molq`` client. The method set matches
``molab.Path``.
"""

from __future__ import annotations

import atexit
import errno
import fnmatch
import os
import pathlib
import re
import secrets
import stat as stat_module
import threading
from base64 import b64decode, b64encode
from collections.abc import Iterator
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any

from molq.errors import MolqError

if TYPE_CHECKING:
    from molq.client.objects import Molq

_REMOTE = re.compile(r"([A-Za-z0-9][A-Za-z0-9_.-]{0,127}):(.*)\Z", re.DOTALL)
_CHUNK = 262144
_REASONS: dict[str, tuple[type[OSError], int]] = {
    "not_found": (FileNotFoundError, errno.ENOENT),
    "exists": (FileExistsError, errno.EEXIST),
    "not_directory": (NotADirectoryError, errno.ENOTDIR),
    "is_directory": (IsADirectoryError, errno.EISDIR),
    "not_empty": (OSError, errno.ENOTEMPTY),
    "outside_root": (PermissionError, errno.EACCES),
}
_KINDS = {"directory": stat_module.S_IFDIR, "file": stat_module.S_IFREG}

_default_lock = threading.Lock()
_default_client: Molq | None = None


def _default_molq() -> Molq:
    """Return the process-wide client used by paths bound to no client."""
    global _default_client
    with _default_lock:
        if _default_client is None:
            from molq.client.objects import Molq

            _default_client = Molq()
            atexit.register(_default_client.close)
        return _default_client


class Path(PurePosixPath):
    """A POSIX path on this machine or on a registered cluster.

    Args:
        *segments (str | os.PathLike): Path segments. A first string segment
            spelled ``CLUSTER:PATH`` names a cluster; ``CLUSTER:relative`` is
            relative to that login's home, and ``CLUSTER:`` is the home itself.
        molq (Molq | None): Client for cluster I/O. None uses one process-wide
            ``Molq()`` created on first use.

    Pure arithmetic (``/``, ``parent``, ``name``, ``with_suffix``) keeps the
    cluster. ``str(path)`` is the ``CLUSTER:PATH`` spelling and round-trips
    through the constructor; ``as_posix()`` is the path on its host. A cluster
    path has no local ``os.fspath``.

    Example:
        run = Path("arrhenius:/scratch/you/run_01")
        energy = (run / "out.log").read_text()
        run.transfer(Path("results/run_01"), "-a")
    """

    __slots__ = ("_cluster", "_molq", "_access_root")

    def __new__(
        cls, *segments: Any, molq: Molq | None = None, root: str | None = None
    ) -> Path:
        return super().__new__(cls)

    def __init__(
        self, *segments: Any, molq: Molq | None = None, root: str | None = None
    ):
        cluster: str | None = None
        parts: list[Any] = list(segments)
        if parts and isinstance(parts[0], Path):
            cluster, molq = parts[0].cluster, molq or parts[0]._molq
            root = root if root is not None else parts[0]._access_root
            parts[0] = parts[0].as_posix()
        elif parts and isinstance(parts[0], str):
            match = _REMOTE.match(parts[0])
            if match:
                cluster, parts[0] = match[1], match[2]
        super().__init__(*(p.as_posix() if isinstance(p, Path) else p for p in parts))
        self._cluster, self._molq, self._access_root = cluster, molq, root

    def with_segments(self, *segments: Any) -> Path:
        """Build a derived path on the same cluster and client."""
        path = type(self).__new__(type(self))
        PurePosixPath.__init__(
            path, *(s.as_posix() if isinstance(s, Path) else s for s in segments)
        )
        path._cluster, path._molq, path._access_root = (
            self._cluster,
            self._molq,
            self._access_root,
        )
        return path

    @property
    def cluster(self) -> str | None:
        """Registered cluster name or ID, or None for this machine."""
        return self._cluster

    def as_posix(self) -> str:
        """Return the path on its host, without the cluster prefix."""
        return PurePosixPath.__str__(self)

    def __str__(self) -> str:
        posix = PurePosixPath.__str__(self)
        return posix if self._cluster is None else f"{self._cluster}:{posix}"

    def __repr__(self) -> str:
        return f"{type(self).__name__}({str(self)!r})"

    def __fspath__(self) -> str:
        if self._cluster is not None:
            raise TypeError(
                f"{self} is on cluster {self._cluster!r}; it has no local path"
            )
        return self.as_posix()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PurePosixPath):
            return NotImplemented
        other_cluster = other.cluster if isinstance(other, Path) else None
        return (self._cluster, self.as_posix()) == (other_cluster, other.as_posix())

    def __hash__(self) -> int:
        return (
            super().__hash__()
            if self._cluster is None
            else hash((self._cluster, self.as_posix()))
        )

    def __reduce__(self) -> tuple[Any, ...]:
        return (
            _restore,
            (type(self), self._cluster, self.as_posix(), self._access_root),
        )

    # ---- I/O ---------------------------------------------------------------

    def _local(self) -> pathlib.Path:
        path = pathlib.Path(self.as_posix())
        if self._access_root is not None:
            root = pathlib.Path(self._access_root).expanduser().resolve()
            if ".." in path.parts or not path.expanduser().resolve().is_relative_to(
                root
            ):
                raise PermissionError(
                    errno.EACCES, "Path is outside the access root", str(self)
                )
        return path

    def _rpc(self, method: str, **params: Any) -> Any:
        client = self._molq or _default_molq()
        try:
            return client.rpc(
                method,
                cluster=self._cluster,
                path=self.as_posix(),
                root=self._access_root,
                **params,
            )
        except MolqError as error:
            mapped = _REASONS.get(error.context.get("reason", ""))
            if mapped is None:
                raise
            kind, code = mapped
            raise kind(code, os.strerror(code), str(self)) from error

    def _same_host(self, target: str | os.PathLike) -> Path:
        other = (
            target
            if isinstance(target, Path)
            else self.with_segments(os.fspath(target))
        )
        if other.cluster != self._cluster:
            raise ValueError(f"{other} is not on the same host as {self}")
        return other

    def stat(self) -> os.stat_result:
        """Return file status, following symlinks.

        Returns:
            os.stat_result: For a cluster path, mode (type and permission
                bits), uid, gid, size and whole-second mtime; the remaining
                fields are zero.

        Raises:
            FileNotFoundError: The path, or a symlink's target, does not exist.
        """
        if self._cluster is None:
            return self._local().stat()
        info = self._rpc("files.stat")
        if not info["exists"]:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(self))
        mode = _KINDS.get(info["kind"], 0) | info["mode"]
        mtime = info["mtime"]
        return os.stat_result(
            (mode, 0, 0, 0, info["uid"], info["gid"], info["size"], mtime, mtime, mtime)
        )

    def exists(self) -> bool:
        """Return whether the path, or a symlink's target, exists."""
        if self._cluster is None:
            return self._local().exists()
        return bool(self._rpc("files.stat")["exists"])

    def is_file(self) -> bool:
        """Return whether the path is a regular file, following symlinks."""
        if self._cluster is None:
            return self._local().is_file()
        return self._rpc("files.stat").get("kind") == "file"

    def is_dir(self) -> bool:
        """Return whether the path is a directory, following symlinks."""
        if self._cluster is None:
            return self._local().is_dir()
        return self._rpc("files.stat").get("kind") == "directory"

    def is_symlink(self) -> bool:
        """Return whether the path itself is a symbolic link."""
        if self._cluster is None:
            return self._local().is_symlink()
        return bool(self._rpc("files.stat")["is_symlink"])

    def iterdir(self) -> Iterator[Path]:
        """Yield the directory's children, including hidden ones, unordered."""
        for name, _kind, _link in self._entries():
            yield self / name

    def _entries(self) -> list[tuple[str, str, bool]]:
        if self._cluster is None:
            out = []
            for child in self._local().iterdir():
                kind = (
                    "directory"
                    if child.is_dir()
                    else "file"
                    if child.is_file()
                    else "other"
                )
                out.append((child.name, kind, child.is_symlink()))
            return out
        entries, after = [], None
        while True:
            page = self.list_page(after=after)
            entries.extend(
                (e["name"], e["kind"], e["is_symlink"]) for e in page["entries"]
            )
            after = page.get("next_after")
            if after is None:
                return entries

    def list_page(
        self, *, with_stat: bool = False, limit: int = 256, after: str | None = None
    ) -> dict:
        """Read one bounded directory page, optionally including metadata."""
        return self._rpc("files.list", with_stat=with_stat, limit=limit, after=after)

    def read_range(self, offset: int, length: int) -> bytes:
        """Read a bounded byte window without materializing the entire file."""
        if offset < 0 or length < 0:
            raise ValueError("offset and length must be non-negative")
        if self._cluster is None:
            with self._local().open("rb") as stream:
                stream.seek(offset)
                return stream.read(length)
        chunks = []
        while length:
            count = min(length, _CHUNK)
            chunk = self._rpc("files.read", offset=offset, limit=count)
            data = b64decode(chunk["data"])
            chunks.append(data)
            offset += len(data)
            length -= len(data)
            if len(data) < count:
                break
        return b"".join(chunks)

    def read_many(self, paths: list[str], *, max_bytes: int = _CHUNK) -> dict:
        """Read explicit paths in one target operation, returning per-item results."""
        client = self._molq or _default_molq()
        return client.rpc(
            "files.read_many",
            cluster=self._cluster,
            root=self._access_root,
            paths=paths,
            max_bytes=max_bytes,
        )

    def resolve(self) -> Path:
        """Resolve the physical destination path under the optional root constraint."""
        if self._cluster is None:
            return type(self)(
                self._local().resolve(), molq=self._molq, root=self._access_root
            )
        return self.with_segments(self._rpc("files.resolve")["path"])

    def chmod(self, mode: int) -> None:
        """Set permission bits on the destination."""
        if self._cluster is None:
            self._local().chmod(mode)
        else:
            self._rpc("files.chmod", mode=mode)

    def symlink_to(self, target: str | os.PathLike) -> None:
        """Create a symlink on the same destination."""
        other = self._same_host(target)
        if self._cluster is None:
            if self._access_root is not None:
                candidate = other if other.is_absolute() else self.parent / other
                self.with_segments(candidate.as_posix())._local()
            self._local().symlink_to(other.as_posix())
        else:
            self._rpc("files.symlink", target=other.as_posix())

    def remove(self, *, recursive: bool = False) -> None:
        """Remove a path; recursive removal must be requested explicitly."""
        if self._cluster is None:
            if recursive:
                import shutil

                shutil.rmtree(self._local())
            else:
                self._local().unlink()
        else:
            self._rpc("files.remove", recursive=recursive)

    def glob(self, pattern: str) -> Iterator[Path]:
        """Yield paths matching a relative pattern; ``**`` spans directories.

        Matching is case-sensitive and ``*`` matches hidden names. ``**`` does
        not descend into symlinked directories. On a cluster each visited
        directory costs one listing RPC.
        """
        segments = [s for s in PurePosixPath(pattern).parts if s != "."]
        if not segments or PurePosixPath(pattern).is_absolute():
            raise ValueError(f"Unacceptable glob pattern: {pattern!r}")
        yield from self._glob(segments)

    def rglob(self, pattern: str) -> Iterator[Path]:
        """Yield matches of ``pattern`` in this directory and every subdirectory."""
        yield from self.glob(f"**/{pattern}")

    def _glob(self, segments: list[str]) -> Iterator[Path]:
        head, rest = segments[0], segments[1:]
        if head == "**":
            if rest:
                yield from self._glob(rest)
            else:
                yield self
            for name, kind, link in self._entries():
                if kind == "directory" and not link:
                    yield from (self / name)._glob(segments)
            return
        for name, kind, _link in self._entries():
            if not fnmatch.fnmatchcase(name, head):
                continue
            if not rest:
                yield self / name
            elif kind == "directory":
                yield from (self / name)._glob(rest)

    def read_bytes(self) -> bytes:
        """Return the whole file, read in bounded RPC chunks on a cluster."""
        if self._cluster is None:
            return self._local().read_bytes()
        info = self._rpc("files.stat")
        if not info["exists"]:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(self))
        if info["kind"] == "directory":
            raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), str(self))
        chunks, offset = [], 0
        while True:
            chunk = self._rpc("files.read", offset=offset, limit=_CHUNK)
            if chunk["bytes_read"] == 0:
                return b"".join(chunks)
            chunks.append(b64decode(chunk["data"]))
            offset = chunk["next_offset"]

    def read_text(
        self, encoding: str | None = "utf-8", errors: str | None = None
    ) -> str:
        """Return the decoded file contents."""
        if self._cluster is None:
            return self._local().read_text(encoding=encoding, errors=errors)
        return self.read_bytes().decode(encoding or "utf-8", errors or "strict")

    def write_bytes(self, data: bytes) -> int:
        """Replace the file with ``data`` and return its length.

        On a cluster, one chunk replaces the file atomically; larger data is
        staged beside it and renamed into place. Missing parents are created.
        """
        if self._cluster is None:
            return self._local().write_bytes(data)
        view = memoryview(data).cast("B")
        if len(view) <= _CHUNK:
            self._rpc("files.write", data=b64encode(view).decode())
            return len(view)
        staged = self.with_name(f".{self.name}.{secrets.token_hex(8)}.part")
        try:
            for offset in range(0, len(view), _CHUNK):
                piece = b64encode(view[offset : offset + _CHUNK]).decode()
                staged._rpc("files.write", data=piece, offset=offset or None)
            staged.replace(self)
        except BaseException:
            try:
                staged.unlink(missing_ok=True)
            except (OSError, MolqError):
                pass
            raise
        return len(view)

    def write_text(
        self,
        data: str,
        encoding: str | None = "utf-8",
        errors: str | None = None,
        newline: str | None = None,
    ) -> int:
        """Replace the file with encoded text and return the character count."""
        if self._cluster is None:
            return self._local().write_text(
                data, encoding=encoding, errors=errors, newline=newline
            )
        text = data.replace("\n", newline) if newline not in (None, "", "\n") else data
        self.write_bytes(text.encode(encoding or "utf-8", errors or "strict"))
        return len(data)

    def mkdir(
        self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        """Create the directory with ``pathlib.Path.mkdir`` semantics."""
        if self._cluster is None:
            self._local().mkdir(mode=mode, parents=parents, exist_ok=exist_ok)
            return
        self._rpc("files.mkdir", mode=mode, parents=parents, exist_ok=exist_ok)

    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        """Create the file, or update its modification time."""
        if self._cluster is None:
            self._local().touch(mode=mode, exist_ok=exist_ok)
            return
        self._rpc("files.touch", exist_ok=exist_ok)

    def unlink(self, missing_ok: bool = False) -> None:
        """Remove a file or symlink."""
        if self._cluster is None:
            self._local().unlink(missing_ok=missing_ok)
            return
        try:
            self._rpc("files.remove")
        except FileNotFoundError:
            if not missing_ok:
                raise

    def rmdir(self) -> None:
        """Remove an empty directory."""
        if self._cluster is None:
            self._local().rmdir()
            return
        self._rpc("files.remove", directory=True)

    def rename(self, target: str | os.PathLike) -> Path:
        """Move this path to ``target`` on the same host and return it.

        A relative cluster target is relative to the login home, like
        ``CLUSTER:relative``.
        """
        other = self._same_host(target)
        if self._cluster is None:
            self._local().rename(other.as_posix())
        else:
            self._rpc("files.rename", target=other.as_posix())
        return other

    def replace(self, target: str | os.PathLike) -> Path:
        """Move this path to ``target``, replacing an existing file."""
        return self.rename(target)

    def transfer(
        self,
        destination: str | os.PathLike,
        *options: str,
        timeout: int = 90,
        directory: bool | None = None,
    ) -> dict:
        """Make ``destination`` a copy of this path with one rsync run.

        A directory is copied as its contents (``SRC/ DEST/`` in rsync terms),
        so ``destination`` becomes the copy, as in ``Path.copy``. Only one
        side may be on a cluster other than the serve host's own.

        Args:
            destination (str | os.PathLike): Target path; a string may be
                spelled ``CLUSTER:PATH``.
            *options (str): rsync options passed unchanged, each one word with
                any value attached (``"--exclude=build/"``). Nothing is implied.

        Returns:
            dict: Runtime transfer result with ``outcome`` and rsync ``output``.

        Raises:
            MolqError: An option is owned by the transport, a path is rejected,
                or rsync fails. An unknown outcome may mean files were copied.
        """
        target = destination if isinstance(destination, Path) else Path(destination)
        is_directory = self.is_dir() if directory is None else directory
        slash = "/" if is_directory else ""
        client = self._molq or target._molq or _default_molq()
        return client.rpc(
            "files.transfer",
            sources=[self._location(slash)],
            destination=target._location(slash),
            options=list(options),
            timeout_seconds=timeout,
        )

    def _location(self, suffix: str) -> dict[str, str]:
        if self._cluster is None:
            return {
                "path": os.path.abspath(self.as_posix()) + suffix,
                **({"root": self._access_root} if self._access_root else {}),
            }
        return {
            "cluster": self._cluster,
            "path": self.as_posix() + suffix,
            **({"root": self._access_root} if self._access_root else {}),
        }


def _restore(
    cls: type[Path], cluster: str | None, posix: str, root: str | None
) -> Path:
    path = cls.__new__(cls)
    PurePosixPath.__init__(path, posix)
    path._cluster, path._molq, path._access_root = cluster, None, root
    return path


__all__ = ["Path"]
