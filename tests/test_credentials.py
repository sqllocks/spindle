"""Tests for CredentialResolver (E12)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from sqllocks_spindle.fabric.credentials import CredentialError, CredentialResolver


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def resolver():
    return CredentialResolver()


@pytest.fixture
def no_cache_resolver():
    return CredentialResolver(cache=False)


# ---------------------------------------------------------------------------
# Raw passthrough
# ---------------------------------------------------------------------------

class TestRawPassthrough:
    def test_plain_string_returned_verbatim(self, resolver):
        assert resolver.resolve("Server=localhost;Database=db") == "Server=localhost;Database=db"

    def test_empty_ref_raises(self, resolver):
        with pytest.raises(CredentialError, match="Empty"):
            resolver.resolve("")


# ---------------------------------------------------------------------------
# env:// scheme
# ---------------------------------------------------------------------------

class TestEnvScheme:
    def test_resolve_existing_env_var(self, resolver, monkeypatch):
        monkeypatch.setenv("SPINDLE_TEST_SECRET", "my_secret_value")
        assert resolver.resolve("env://SPINDLE_TEST_SECRET") == "my_secret_value"

    def test_missing_env_var_raises(self, resolver):
        # Ensure it doesn't exist
        os.environ.pop("SPINDLE_MISSING_VAR_XYZ", None)
        with pytest.raises(CredentialError, match="not set"):
            resolver.resolve("env://SPINDLE_MISSING_VAR_XYZ")

    def test_empty_var_name_raises(self, resolver):
        with pytest.raises(CredentialError, match="missing variable name"):
            resolver.resolve("env://")


# ---------------------------------------------------------------------------
# file:// scheme
# ---------------------------------------------------------------------------

class TestFileScheme:
    def test_resolve_file(self, resolver, tmp_path):
        secret_file = tmp_path / "secret.txt"
        secret_file.write_text("super_secret_123\n", encoding="utf-8")
        result = resolver.resolve(f"file://{secret_file}")
        assert result == "super_secret_123"

    def test_reads_first_line_only(self, resolver, tmp_path):
        secret_file = tmp_path / "multi.txt"
        secret_file.write_text("line1\nline2\nline3\n", encoding="utf-8")
        result = resolver.resolve(f"file://{secret_file}")
        assert result == "line1"

    def test_missing_file_raises(self, resolver):
        with pytest.raises(CredentialError, match="not found"):
            resolver.resolve("file:///nonexistent/path/secret.txt")

    def test_empty_file_raises(self, resolver, tmp_path):
        empty_file = tmp_path / "empty.txt"
        empty_file.write_text("", encoding="utf-8")
        with pytest.raises(CredentialError, match="empty"):
            resolver.resolve(f"file://{empty_file}")

    def test_empty_path_raises(self, resolver):
        with pytest.raises(CredentialError, match="missing path"):
            resolver.resolve("file://")


# ---------------------------------------------------------------------------
# kv:// scheme
# ---------------------------------------------------------------------------

class TestKvScheme:
    def test_invalid_kv_format_raises(self, resolver):
        with pytest.raises(CredentialError, match="expected kv://"):
            resolver.resolve("kv://just-vault-no-secret")

    def test_empty_vault_raises(self, resolver):
        with pytest.raises(CredentialError, match="expected kv://"):
            resolver.resolve("kv:///secret-name")

    def test_empty_secret_raises(self, resolver):
        with pytest.raises(CredentialError, match="expected kv://"):
            resolver.resolve("kv://vault/")

    def test_missing_azure_packages_raises(self, resolver, monkeypatch):
        # Simulate missing azure packages by patching the import
        import builtins
        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if "azure" in name:
                raise ImportError("No module named 'azure'")
            return original_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", mock_import)
        # Clear the resolver cache to ensure fresh resolve
        resolver.clear_cache()
        with pytest.raises(CredentialError, match="azure-identity"):
            resolver.resolve("kv://my-vault/my-secret")


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------

class TestCaching:
    def test_cached_value_returned(self, resolver, monkeypatch):
        monkeypatch.setenv("SPINDLE_CACHE_TEST", "value1")
        assert resolver.resolve("env://SPINDLE_CACHE_TEST") == "value1"
        # Change the env var — cached resolver should still return old value
        monkeypatch.setenv("SPINDLE_CACHE_TEST", "value2")
        assert resolver.resolve("env://SPINDLE_CACHE_TEST") == "value1"

    def test_no_cache_returns_fresh(self, no_cache_resolver, monkeypatch):
        monkeypatch.setenv("SPINDLE_NOCACHE_TEST", "value1")
        assert no_cache_resolver.resolve("env://SPINDLE_NOCACHE_TEST") == "value1"
        monkeypatch.setenv("SPINDLE_NOCACHE_TEST", "value2")
        assert no_cache_resolver.resolve("env://SPINDLE_NOCACHE_TEST") == "value2"

    def test_clear_cache(self, resolver, monkeypatch):
        monkeypatch.setenv("SPINDLE_CLEAR_TEST", "old")
        resolver.resolve("env://SPINDLE_CLEAR_TEST")
        monkeypatch.setenv("SPINDLE_CLEAR_TEST", "new")
        resolver.clear_cache()
        assert resolver.resolve("env://SPINDLE_CLEAR_TEST") == "new"
