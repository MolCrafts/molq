# Installation

molq requires Python 3.12 or later.

```sh
pip install molcrafts-molq
molq --version
```

The package includes the `molq` command and the compiled service. Wheels exist
for Linux (glibc or musl) and Windows on x86_64 and arm64, and for macOS 12 or
later on Intel and Apple silicon. Other platforms install from source, which
requires Go; the compiler version is listed in `go.mod`.

## On your machine

Local jobs require Bash on Linux or macOS. Remote jobs require system OpenSSH;
file transfers also require rsync. Windows supports the remote control plane.

The target must already have your program and its dependencies. Slurm, PBS, and
LSF commands run on the target account; molq does not install them.

## TypeScript

The SDK requires Node.js 22 or later for its Node transport.

```sh
npm install @molcrafts/molq
```

For HTTP connections, start a [shared service](configuration.md#shared-service).
Node can also start the installed `molq` executable through `StdioWire`.
See the [TypeScript guide](reference/typescript.md).

## From source

```sh
git clone https://github.com/MolCrafts/molq.git
cd molq
uv sync --extra dev --extra docs
```

Build the manual locally:

```sh
uv run --extra docs zensical serve
```

Upgrading from 0.8? Read the [0.9.0 release notes](release-notes.md) first.
