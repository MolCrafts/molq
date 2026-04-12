# Releasing

This repository uses a static version in `pyproject.toml`.

## PyPI Trusted Publisher

This repository is set up to publish through PyPI trusted publishing from GitHub Actions.
No PyPI API token should be stored in GitHub secrets for the normal release flow.

Configure the trusted publisher in PyPI with:

- Project name: `molcrafts-molq`
- Owner: `MolCrafts`
- Repository: `molq`
- Workflow: `release.yml`
- Environment: `pypi`

The GitHub repository must also have an environment named `pypi`.

## Release Checklist

1. Ensure `pyproject.toml` has the intended version.
2. Update `CHANGELOG.md` with the release date and notable changes.
3. Run the test suite:

   ```bash
   pytest -q
   ```

4. Build distributions:

   ```bash
   python -m build
   ```

5. Optionally verify artifacts:

   ```bash
   python -m pip install twine
   python -m twine check dist/*
   ```

6. Tag the release:

   ```bash
   git tag v0.1.0
   git push origin v0.1.0
   ```

7. Wait for the `Release` workflow to publish the artifacts to PyPI.
8. Publish a GitHub release for the tag and paste the changelog entry into the release notes.

## Documentation Release

If documentation dependencies are installed:

```bash
zensical build
```

Deploy the generated `site/` directory with your preferred static host.
