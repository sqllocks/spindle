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
