# Releasing

0.9.0 replaces the 0.8 API without compatibility or automatic jobs.db migration. It does not delete user data or native tasks.

1. Update the version in pyproject.toml, molq.__version__, the binary release, the protocol release and sdk/typescript/package.json/package-lock.json. Regenerate the contract and client types.
2. Run tools/check_generated.py, Ruff, ty, Python tests with coverage and TypeScript conformance.
3. Run tools/build_wheels.py --sdist. It cross-builds one py3-none wheel per platform (Linux glibc/musl, macOS 12+, Windows; x86_64 and arm64) and checks each wheel's tag and binary header. Install a wheel into a clean environment and run tools/check_wheel.py outside the checkout. Check package metadata and npm pack contents. The release workflow publishes the artifacts its build job produced with the same script.
4. Confirm Linux/macOS and Windows control-plane CI. Document native Scheduler mapping limits and actual live-cluster evidence.
5. Tag only after validation. The existing v* release workflow requires reusable CI and matching package version before PyPI trusted publishing. A master push alone does not publish a package.

TypeScript publishing is a separate explicit registry release; building/testing the package does not publish it.
