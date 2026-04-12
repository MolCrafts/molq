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
black --check src tests
isort --check-only src tests
pytest -q
```

If docs dependencies are installed, you can preview the site locally:

```bash
zensical serve
```

## Change Expectations

- Keep public API changes intentional and documented.
- Add or update tests for behavior changes and bug fixes.
- Update `README.md`, `docs/`, and `CHANGELOG.md` when public behavior changes.
- Keep scheduler-specific changes explicit about which backend they affect.

## Pull Requests

- Keep each PR scoped to one logical change.
- Describe user-visible impact clearly.
- Include migration notes if existing user code needs to change.
