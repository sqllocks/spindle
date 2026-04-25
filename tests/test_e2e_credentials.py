"""E2E tests: CredentialResolver — env://, file://, raw passthrough, caching."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from sqllocks_spindle.fabric.credentials import CredentialResolver, CredentialError


class TestCredentialResolver:
    def test_raw_passthrough(self):
        resolver = CredentialResolver()
        assert resolver.resolve("my-secret-value") == "my-secret-value"

    def test_env_resolution(self, monkeypatch):
        monkeypatch.setenv("SPINDLE_TEST_SECRET", "env-secret-123")
        resolver = CredentialResolver()
        assert resolver.resolve("env://SPINDLE_TEST_SECRET") == "env-secret-123"

    def test_env_missing_raises(self):
        resolver = CredentialResolver()
        with pytest.raises(CredentialError):
            resolver.resolve("env://NONEXISTENT_VAR_XYZ_12345")

    def test_file_resolution(self, tmp_path):
        secret_file = tmp_path / "secret.txt"
        secret_file.write_text("file-secret-456\n")
        resolver = CredentialResolver()
        result = resolver.resolve(f"file://{secret_file}")
        assert result == "file-secret-456"

    def test_caching_returns_same_value(self, monkeypatch):
        monkeypatch.setenv("SPINDLE_CACHE_TEST", "cached-value")
        resolver = CredentialResolver(cache=True)
        v1 = resolver.resolve("env://SPINDLE_CACHE_TEST")
        v2 = resolver.resolve("env://SPINDLE_CACHE_TEST")
        assert v1 == v2 == "cached-value"
