# Contributing to Spindle

Thanks for your interest in contributing to Spindle! This document covers how to get started.

## Development Setup

```bash
# Clone the repo
git clone https://github.com/sqllocks/spindle.git
cd spindle

# Install in development mode with all extras
pip install -e ".[all,dev]"

# Run tests
pytest tests/ -v
```

## Running Tests

```bash
# Full suite
pytest tests/ -v

# Specific test file
pytest tests/test_e2e_generation.py -v

# With coverage
pytest tests/ --cov=sqllocks_spindle --cov-report=term-missing
```

## Adding a New Domain

A domain is a reusable schema + calibration package. The fastest way to contribute to Spindle is to add one.

1. Copy the scaffold:

   ```bash
   cp -r domains/_template/ domains/<your-domain>/
   ```

2. Define entities, attributes, and relationships in the `.spindle.json` schema. Existing domains (`domains/retail/`, `domains/financial/`, `domains/healthcare/`) are good reference.

3. Cite calibration sources. Spindle is statistically calibrated — distributions should come from public stats (BLS, NAIC, NCES, NAR, FDIC, industry reports, etc.), not from assumption. Document each source in `domains/<your-domain>/methodology.md`.

4. Add a smoke test under `tests/domains/test_<your-domain>.py` that generates ~1,000 rows and asserts FK integrity:

   ```python
   def test_smoke():
       result = Spindle().generate(YourDomain(), scale="small", seed=42)
       assert result.verify_integrity() == []
   ```

5. Add a short entry to `docs/domains/<your-domain>.md` describing the schema and use cases.

6. Open a PR. Tag it `domain` so it shows up in the domain queue.

Domains we'd love to see: bookstore, events, library, conference, gym, telecom, gaming, logistics. Anything practitioner-shaped is welcome — keep it 5–10 tables, statistically realistic, and self-contained.

If you'd rather discuss the idea before writing code, [open a Discussion](https://github.com/sqllocks/spindle/discussions) or comment on [Issue #1](https://github.com/sqllocks/spindle/issues/1).

## Adding a Custom Strategy

Spindle supports entrypoint-based plugins. To add a custom strategy:

1. Create a class extending `sqllocks_spindle.engine.strategies.base.Strategy`
2. Implement the `generate(column, config, ctx) -> np.ndarray` method
3. Register via entrypoint in your `pyproject.toml`:

```toml
[project.entry-points."spindle.strategies"]
my_strategy = "my_package.strategies:MyStrategy"
```

## Code Style

- Python 3.10+ type hints
- Use `logging.getLogger(__name__)` (not `print()`)
- Use `datetime.now(UTC)` (not `datetime.utcnow()`)
- Tests go in `tests/` with `test_` prefix
- Use `pytest` fixtures, not unittest classes

## Reporting Issues

Please open an issue on GitHub with:
- Python version and OS
- Spindle version (`spindle --version`)
- Minimal reproduction steps
- Full error traceback

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
