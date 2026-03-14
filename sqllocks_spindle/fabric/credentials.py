"""Credential resolver — resolve secrets from env vars, Key Vault, files, or raw values.

Supports URI-style credential references so that connection strings and
secrets are never hard-coded:

* ``env://VARIABLE_NAME`` — read from environment variable
* ``kv://vault-name/secret-name`` — read from Azure Key Vault (requires ``azure-identity`` + ``azure-keyvault-secrets``)
* ``file:///path/to/secret.txt`` — read from a local file (first line, stripped)
* Any other value — passed through as-is (raw)

Usage::

    from sqllocks_spindle.fabric.credentials import CredentialResolver

    resolver = CredentialResolver()
    conn_str = resolver.resolve("env://SPINDLE_SQL_CONNECTION")
    secret   = resolver.resolve("kv://my-vault/my-secret")
    raw      = resolver.resolve("Server=localhost;Database=db")
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class CredentialResolver:
    """Resolve credential references into concrete secret values.

    The resolver supports four URI schemes:

    * ``env://VAR`` — environment variable lookup
    * ``kv://vault/secret`` — Azure Key Vault lookup (lazy import)
    * ``file:///path`` — read secret from a local file
    * Anything else — returned verbatim (raw passthrough)

    Args:
        cache: If ``True`` (default), resolved values are cached for the
            lifetime of the resolver instance so that repeated lookups
            (especially Key Vault) are fast.
    """

    def __init__(self, *, cache: bool = True) -> None:
        self._cache_enabled = cache
        self._cache: dict[str, str] = {}

    def resolve(self, ref: str) -> str:
        """Resolve a credential reference to its concrete value.

        Args:
            ref: A credential URI (``env://``, ``kv://``, ``file://``) or
                a raw value string.

        Returns:
            The resolved secret string.

        Raises:
            CredentialError: If the reference cannot be resolved.
        """
        if not ref:
            raise CredentialError("Empty credential reference")

        if self._cache_enabled and ref in self._cache:
            return self._cache[ref]

        value = self._resolve_inner(ref)

        if self._cache_enabled:
            self._cache[ref] = value

        return value

    def clear_cache(self) -> None:
        """Clear all cached credential values."""
        self._cache.clear()

    # ------------------------------------------------------------------
    # Internal dispatch
    # ------------------------------------------------------------------

    def _resolve_inner(self, ref: str) -> str:
        if ref.startswith("env://"):
            return self._resolve_env(ref)
        if ref.startswith("kv://"):
            return self._resolve_kv(ref)
        if ref.startswith("file://"):
            return self._resolve_file(ref)
        # Raw passthrough
        return ref

    # ------------------------------------------------------------------
    # Scheme handlers
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_env(ref: str) -> str:
        """Resolve ``env://VARIABLE_NAME``."""
        var_name = ref[len("env://"):]
        if not var_name:
            raise CredentialError("env:// reference missing variable name")
        value = os.environ.get(var_name)
        if value is None:
            raise CredentialError(
                f"Environment variable {var_name!r} not set"
            )
        logger.debug("Resolved credential from env://%s", var_name)
        return value

    @staticmethod
    def _resolve_kv(ref: str) -> str:
        """Resolve ``kv://vault-name/secret-name``."""
        path = ref[len("kv://"):]
        parts = path.split("/", 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise CredentialError(
                f"Invalid kv:// reference {ref!r} — expected kv://vault-name/secret-name"
            )
        vault_name, secret_name = parts

        try:
            from azure.identity import DefaultAzureCredential
            from azure.keyvault.secrets import SecretClient
        except ImportError as exc:
            raise CredentialError(
                "kv:// references require azure-identity and azure-keyvault-secrets packages. "
                "Install with: pip install azure-identity azure-keyvault-secrets"
            ) from exc

        vault_url = f"https://{vault_name}.vault.azure.net"
        client = SecretClient(vault_url=vault_url, credential=DefaultAzureCredential())
        secret = client.get_secret(secret_name)
        logger.debug("Resolved credential from kv://%s/%s", vault_name, secret_name)
        return secret.value

    @staticmethod
    def _resolve_file(ref: str) -> str:
        """Resolve ``file:///path/to/secret.txt`` — reads first line, stripped."""
        file_path = ref[len("file://"):]
        if not file_path:
            raise CredentialError("file:// reference missing path")
        path = Path(file_path)
        if not path.is_file():
            raise CredentialError(f"Credential file not found: {path}")
        text = path.read_text(encoding="utf-8").strip().splitlines()
        if not text:
            raise CredentialError(f"Credential file is empty: {path}")
        logger.debug("Resolved credential from file://%s", file_path)
        return text[0].strip()


class CredentialError(Exception):
    """Raised when a credential reference cannot be resolved."""
