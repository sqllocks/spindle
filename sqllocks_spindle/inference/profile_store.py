"""ProfileStore — the single supported on-disk path for SafeProfile (STORY-003).

This module is the **canonical, only-supported public on-disk entrypoint** for
persisting and retrieving a :class:`~sqllocks_spindle.inference.safe_profile.SafeProfile`
(ADR-001 / ADR-007). The rich in-memory ``DatasetProfile`` (``profiler.py``) is
the *source*; ``SafeProfile`` is the *transport*; ``ProfileStore`` is the *door*
to disk.

Public API
----------

* :meth:`ProfileStore.save` — write a ``SafeProfile`` to a JSON file via its
  ``to_safe_dict`` representation. Returns the resolved path.
* :meth:`ProfileStore.load` — read a JSON file written by ``save`` and return a
  reconstructed ``SafeProfile``. A file whose ``schema_version`` is unknown to
  the current code (legacy ``0`` or any future version) is loaded **read-only
  with a warning** — never a crash (ADR-001: legacy files load read-only as
  ``schema_version=0``).

There is no other public on-disk path. Legacy serialization paths
(``ProfileRegistry``, ``ProfileIO``) are re-pointed at ``ProfileStore`` in
STORY-014; until then they remain but are not the supported safe path.

Scope note (STORY-003): this story owns the save/load round-trip + legacy
``schema_version`` handling.

Safe-by-default (STORY-009 / ADR-005): the scrub (winsorize + k-anon + PII
gate) runs at MAPPING time (``SafeProfile.from_dataset_profile``), so a
``SafeProfile`` reaching ``save`` is already safe-by-construction. ``save``
writes the profile's ``unsafe`` stamp and embedded ``redaction_manifest`` as-is
— a ``SafeProfile`` built with ``unsafe_full_fidelity=True`` is persisted with
``"unsafe": true`` in the artifact (and rejected later by ``validate --safe``,
STORY-010). ``SafeProfile`` carries no raw-bearing fields by construction
(STORY-001 / ADR-007), so there is nothing here to refuse.
"""

from __future__ import annotations

import json
import warnings
from pathlib import Path

from sqllocks_spindle.inference.safe_profile import SCHEMA_VERSION, SafeProfile

__all__ = ["ProfileStore"]


class ProfileStore:
    """Persist and retrieve a :class:`SafeProfile` to/from a JSON file.

    All methods are stateless — instantiate with ``ProfileStore()`` and call
    directly, or use the classmethods. This is the only supported public
    on-disk entrypoint for a ``SafeProfile`` (ADR-001 / ADR-007).
    """

    # ------------------------------------------------------------------
    # Save
    # ------------------------------------------------------------------

    @classmethod
    def save(cls, profile: SafeProfile, path: str | Path) -> Path:
        """Write ``profile`` to ``path`` as JSON (via ``to_safe_dict``).

        Args:
            profile: The :class:`SafeProfile` to persist. By construction it
                carries no raw-bearing fields (ADR-007), so the on-disk JSON is
                safe-by-construction.
            path: Destination file path. Parent directories are created.

        Returns:
            The resolved :class:`Path` the profile was written to.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        data = profile.to_safe_dict()
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=False)
        return path

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    @classmethod
    def load(cls, path: str | Path) -> SafeProfile:
        """Read a ``SafeProfile`` from a JSON file written by :meth:`save`.

        A file whose ``schema_version`` is not the version this code writes
        (e.g. a legacy artifact with no/old version, or a future version) is
        loaded **read-only with a warning** — it never crashes. The returned
        object is degraded-but-usable: the keys present are reconstructed, and
        the loaded ``schema_version`` is preserved on the returned object so a
        caller can detect the mismatch.

        Args:
            path: Path to a JSON file previously written by :meth:`save`.

        Returns:
            A reconstructed :class:`SafeProfile`.
        """
        path = Path(path)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Legacy artifacts (e.g. the buggy registry export) may have no
        # ``schema_version`` key at all — treat that as the legacy version 0
        # (ADR-001: legacy files load read-only as schema_version=0).
        file_version = data.get("schema_version", 0)

        if file_version != SCHEMA_VERSION:
            warnings.warn(
                f"Loading SafeProfile from {path} with schema_version="
                f"{file_version!r}, but this code writes schema_version="
                f"{SCHEMA_VERSION}. Loading read-only (degraded-but-usable); "
                f"re-profile the source data to upgrade.",
                stacklevel=2,
            )

        profile = SafeProfile.from_safe_dict(data)
        # Preserve the on-disk version so callers can detect the mismatch
        # instead of silently seeing the current SCHEMA_VERSION default.
        profile.schema_version = file_version
        return profile
