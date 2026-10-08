# Contributing

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

If you want to work on the documentation site:

```bash
pip install -e ".[docs]"
```

## Local Checks

Run the standard checks before opening a pull request:

```bash
ruff check src tests
ruff format --check src tests
ty check src/
pytest -q
```

If docs dependencies are installed, you can preview the site locally:

```bash
zensical serve
```

## CI

One workflow per kind of work; shared setup comes from
`MolCrafts/molcrafts-ci/actions/<name>@master`. Each workflow's first job,
`<file> / context`, runs `MolCrafts/molcrafts-ci/actions/ci-context`, and every
other job gates on its outputs; `test / context` picks the tier:
the *fast* tier runs on a feature-branch push to MolCrafts; the *full* tier on
every push to a fork (so a branch is proven before its pull request), on
`dev`/`master`/`main` on MolCrafts, on pull requests, tags and dispatches. A
pull request inside a fork is skipped (its push already ran the full tier).

| workflow | fast tier (feature branch on MolCrafts) | full tier (fork pushes, dev/master/main, PRs, tags) | MolCrafts only |
|---|---|---|---|
| `lint.yml` | `lint / hooks` (pre-commit stage, all files), `lint / workflows` (MolCrafts/molcrafts-ci/actions/check-workflows) | same | — |
| `test.yml` | `test / context`, `test / python (ubuntu-latest, 3.12)`, `test / package` | `test / context`, `test / python ({ubuntu,macos}-latest, {3.12,3.13})`, `test / package` | — |
| `docs.yml` | `docs / build` (`zensical build --strict`) | same | deploy: Cloudflare Pages, outside Actions |
| `release.yml` | — | — | `v*` tag: lint + test + `release / build` + `release / pypi` (only a `v*` tag on MolCrafts: `publish`); `workflow_dispatch` = dry run (no upload) |

The `protect-master` ruleset on `master` requires a pull request, blocks force
pushes and deletion, and requires `test / context` and the full-tier `lint /`, `test /` and `docs /` checks.


## Change Expectations

- Keep public API changes intentional and documented.
- Add or update tests for behavior changes and bug fixes.
- Update `README.md` and `docs/` when public behavior changes.
  Release history lives in git tags / GitHub Releases (no `CHANGELOG.md`).
- Keep scheduler-specific changes explicit about which backend they affect.

## Pull Requests

- Keep each PR scoped to one logical change.
- Describe user-visible impact clearly.
- Include migration notes if existing user code needs to change.
