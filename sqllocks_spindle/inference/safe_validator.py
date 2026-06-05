"""SafeProfileValidator — structural, fail-closed static leak scanner (STORY-010).

ADR-006: a static scanner over a *serialized* artifact (never the live data),
usable as a pre-commit / CI gate. Non-zero exit on any hit.

PO rewrite (2026-06-04) — STRUCTURAL deny rules, not a name/shape allowlist
--------------------------------------------------------------------------
The first implementation was security-refuted: it only matched literal field
names (``min_value``/``max_value``/…) and only walked a top-level ``"tables"``
key, so five bypasses passed clean. This validator is **structural and
fail-closed**:

* **Walk every dict/list node recursively** — it does not depend on a
  ``"tables"`` key or on columns being dicts. Legacy ``RegistryProfile`` JSON
  (top-level ``columns``, no ``"tables"``) and list-shaped columns are still
  fully scanned.
* **Deny by shape, not by name:**

  - any list of more than ``k`` raw strings (catches ``top_values`` /
    ``samples`` / any key);
  - any numeric **extreme-pair** — a ``min``/``max``-ish pair under ANY parent
    (e.g. ``bounds.min``/``bounds.max``, ``min_value``/``max_value``,
    ``min``/``max``);
  - any value matching a PII regex (SSN / email / phone / IP / IBAN) anywhere.

* **Allowlist resolution (PO decision 2026-06-04, Option A):** "no name-only
  allowlisting" forbids relying on names to *catch* a leak (deny rules are
  structural). It does NOT forbid a tight, closed ALLOWLIST of schema-known
  **safe-aggregate containers**. The ONLY safe containers that legitimately
  carry bare ``min``/``max`` are ``string_length`` and ``length_dist``
  (len()-derived aggregates — never raw values; ``bounds`` uses ``lo``/``hi``,
  not min/max). So a ``min``/``max`` extreme-pair is EXEMPT **only** when its
  immediate parent key is ``string_length`` or ``length_dist``; under ANY other
  parent it is FLAGGED. This passes a safe ``SafeProfile`` artifact (whose
  PII-gated columns carry ``length_dist`` with min/max length aggregates) while
  flagging the legacy ``RegistryProfile`` raw-value leak (bare ``min``/``max``
  directly under a column, not inside a length container).

* **Fail-CLOSED on ambiguity:** if a table's ``row_count`` is absent / unknown,
  a node's safety can't be determined, or the artifact lacks ``SafeProfile``
  schema markers (legacy / foreign JSON), the artifact is FLAGGED — never
  skipped. The only "safe" exits are artifacts proven clean.

* The robust ``unsafe=true`` stamp check is retained (ADR-005).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

__all__ = ["SafeProfileValidator", "ValidationFinding", "ValidationResult"]


# ---------------------------------------------------------------------------
# Deny-rule knobs
# ---------------------------------------------------------------------------

# A list of more than this many raw strings is a leak (raw value list — catches
# ``top_values`` / ``samples`` / any key, name-independently). The bound is the
# k-anon suppression floor: a safe categorical can legitimately carry a handful
# of high-frequency keys (post-suppression weights), but a *list of bare
# strings* of any length beyond a couple of structural sentinels is a raw dump.
# We deny lists of strings with length > MAX_RAW_STRING_LIST.
MAX_RAW_STRING_LIST = 2

# Keys whose IMMEDIATE-parent context makes a bare ``min``/``max`` pair a safe
# length aggregate (Option A closed allowlist). NOTHING else exempts min/max.
SAFE_MINMAX_CONTAINERS = frozenset({"string_length", "length_dist"})

# The ``min``/``max``-ish key spellings that form an extreme-pair. Detection is
# structural: any parent dict carrying BOTH a min-ish and a max-ish numeric key
# is an extreme-pair (unless its key in SAFE_MINMAX_CONTAINERS).
_MIN_KEYS = frozenset({"min", "min_value", "minimum"})
_MAX_KEYS = frozenset({"max", "max_value", "maximum"})

# SafeProfile schema markers — at least one must be present for an artifact to
# be a recognised SafeProfile. Absence => legacy/foreign => fail-closed FLAG.
SAFE_SCHEMA_MARKERS = frozenset({"schema_version", "redaction_manifest"})


# ---------------------------------------------------------------------------
# PII regexes (un-anchored: match PII embedded ANYWHERE in a string value)
# ---------------------------------------------------------------------------
# The profiler's regexes are full-string anchored (``^...$``) for *classifying*
# a whole column; here we need to catch a PII substring leaking inside any
# value anywhere in the artifact, so these are deliberately un-anchored search
# patterns covering the classes the story enumerates: SSN / email / phone / IP
# / IBAN.
_PII_REGEXES: dict[str, re.Pattern[str]] = {
    "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "email": re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"),
    # IPv4 dotted-quad with bounded octets.
    "ip": re.compile(
        r"\b(?:25[0-5]|2[0-4]\d|[01]?\d?\d)"
        r"(?:\.(?:25[0-5]|2[0-4]\d|[01]?\d?\d)){3}\b"
    ),
    # IBAN: 2-letter country + 2 check digits + 11-30 alnum.
    "iban": re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b"),
    # Phone: + or digit groups with separators, 7-15 digits total. Anchored to
    # a plausible phone shape to avoid flagging arbitrary digit runs.
    "phone": re.compile(
        r"(?<!\d)(?:\+?\d[\d\-\.\s\(\)]{6,18}\d)(?!\d)"
    ),
}

# A phone candidate must contain at least this many digits to count (the loose
# phone shape would otherwise match short numeric tokens).
_PHONE_MIN_DIGITS = 7
_PHONE_MAX_DIGITS = 15


@dataclass
class ValidationFinding:
    """A single leak finding, with the JSON path that triggered it."""

    rule: str
    path: str
    detail: str

    def to_dict(self) -> dict[str, str]:
        return {"rule": self.rule, "path": self.path, "detail": self.detail}


@dataclass
class ValidationResult:
    """Outcome of a scan. ``is_clean`` only when zero findings."""

    path: str
    findings: list[ValidationFinding] = field(default_factory=list)

    @property
    def is_clean(self) -> bool:
        return not self.findings

    @property
    def exit_code(self) -> int:
        """0 only on a proven-clean artifact; 1 on any finding."""
        return 0 if self.is_clean else 1

    def add(self, rule: str, path: str, detail: str) -> None:
        self.findings.append(ValidationFinding(rule=rule, path=path, detail=detail))

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "clean": self.is_clean,
            "exit_code": self.exit_code,
            "findings": [f.to_dict() for f in self.findings],
        }


class SafeProfileValidator:
    """Structural, fail-closed static leak scanner over a serialized artifact.

    Usage::

        result = SafeProfileValidator().validate_file("profile.json")
        sys.exit(result.exit_code)
    """

    def __init__(
        self,
        max_raw_string_list: int = MAX_RAW_STRING_LIST,
    ) -> None:
        self.max_raw_string_list = max_raw_string_list

    # ------------------------------------------------------------------
    # Entry points
    # ------------------------------------------------------------------

    def validate_file(self, path: str | Path) -> ValidationResult:
        """Load and scan a JSON artifact file. Fail-closed on any read error."""
        path = Path(path)
        result = ValidationResult(path=str(path))
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            result.add("unreadable", str(path), f"cannot read artifact: {exc}")
            return result
        try:
            data = json.loads(text)
        except (json.JSONDecodeError, ValueError) as exc:
            result.add("malformed", str(path), f"not valid JSON: {exc}")
            return result
        return self.validate_data(data, path=str(path))

    def validate_data(self, data: Any, path: str = "<data>") -> ValidationResult:
        """Scan an already-parsed artifact. Fail-closed on missing markers."""
        result = ValidationResult(path=path)

        # ---- Schema-marker gate (fail-closed on legacy / foreign JSON) ----
        if not (isinstance(data, dict) and (SAFE_SCHEMA_MARKERS & data.keys())):
            result.add(
                "not-safe-profile",
                "$",
                "artifact lacks SafeProfile schema markers "
                f"({sorted(SAFE_SCHEMA_MARKERS)}); legacy/foreign JSON is "
                "flagged fail-closed (PO rule: only proven-clean artifacts pass)",
            )
            # Still walk it: surface the concrete leaks too (legacy raw min/max,
            # top_values lists, embedded PII) so the report is actionable.

        # ---- unsafe=true stamp (ADR-005) ----
        if isinstance(data, dict) and bool(data.get("unsafe", False)) is True:
            result.add(
                "unsafe-stamp",
                "$.unsafe",
                "artifact stamped unsafe=true (full-fidelity opt-out); rejected "
                "by validate --safe (ADR-005)",
            )

        # ---- row_count presence (fail-closed) ----
        self._check_row_counts(data, result)

        # ---- recursive structural walk ----
        self._walk(data, "$", parent_key=None, result=result)

        return result

    # ------------------------------------------------------------------
    # row_count fail-closed check
    # ------------------------------------------------------------------

    def _check_row_counts(self, data: Any, result: ValidationResult) -> None:
        """Every table node must carry a usable ``row_count`` or it is FLAGGED.

        A SafeProfile carries ``tables[<name>].row_count``. If the artifact has
        a ``tables`` mapping, every table must have a positive integer
        ``row_count`` — absent/unknown/non-positive => fail-closed FLAG (a
        node's safety can't be determined without it).
        """
        if not isinstance(data, dict):
            return
        tables = data.get("tables")
        if not isinstance(tables, dict):
            return
        for tname, tnode in tables.items():
            if not isinstance(tnode, dict):
                result.add(
                    "row-count-missing",
                    f"$.tables.{tname}",
                    "table node is not an object; row_count undeterminable",
                )
                continue
            rc = tnode.get("row_count")
            if not isinstance(rc, int) or isinstance(rc, bool) or rc <= 0:
                result.add(
                    "row-count-missing",
                    f"$.tables.{tname}.row_count",
                    f"row_count absent/unknown/non-positive ({rc!r}); node "
                    "safety undeterminable — flagged fail-closed",
                )

    # ------------------------------------------------------------------
    # Recursive structural walk
    # ------------------------------------------------------------------

    def _walk(
        self,
        node: Any,
        path: str,
        parent_key: str | None,
        result: ValidationResult,
    ) -> None:
        """Walk every dict/list node, applying the deny-by-shape rules."""
        if isinstance(node, dict):
            # Extreme-pair check on THIS dict (its own keys), with the
            # immediate-parent allowlist (Option A).
            self._check_extreme_pair(node, path, parent_key, result)
            for key, value in node.items():
                child_path = f"{path}.{key}"
                # String leaf: PII scan.
                if isinstance(value, str):
                    self._check_pii_string(value, child_path, result)
                self._walk(value, child_path, parent_key=key, result=result)
        elif isinstance(node, list):
            # Raw-string-list check (deny lists of > k raw strings under ANY key).
            self._check_raw_string_list(node, path, parent_key, result)
            for idx, item in enumerate(node):
                child_path = f"{path}[{idx}]"
                if isinstance(item, str):
                    self._check_pii_string(item, child_path, result)
                # Recurse into nested structures; parent_key unchanged (the list
                # belongs to parent_key).
                self._walk(item, child_path, parent_key=parent_key, result=result)
        elif isinstance(node, str):
            self._check_pii_string(node, path, result)
        # scalars (int/float/bool/None) carry no structural leak on their own.

    # ------------------------------------------------------------------
    # Deny rules
    # ------------------------------------------------------------------

    def _check_extreme_pair(
        self,
        node: dict,
        path: str,
        parent_key: str | None,
        result: ValidationResult,
    ) -> None:
        """Flag a numeric min/max extreme-pair under any parent except the
        closed safe-container allowlist (``string_length`` / ``length_dist``)."""
        has_min = any(
            k in node and self._is_number(node[k]) for k in _MIN_KEYS
        )
        has_max = any(
            k in node and self._is_number(node[k]) for k in _MAX_KEYS
        )
        if not (has_min and has_max):
            return
        # Option A exemption: only when the IMMEDIATE parent key is a known safe
        # length-aggregate container.
        if parent_key in SAFE_MINMAX_CONTAINERS:
            return
        min_key = next(k for k in _MIN_KEYS if k in node and self._is_number(node[k]))
        max_key = next(k for k in _MAX_KEYS if k in node and self._is_number(node[k]))
        result.add(
            "extreme-pair",
            path,
            f"numeric extreme-pair ({min_key}={node[min_key]!r}, "
            f"{max_key}={node[max_key]!r}) under parent "
            f"{parent_key!r}; raw min/max is a re-identifiable leak (ADR-002). "
            f"Exempt only under {sorted(SAFE_MINMAX_CONTAINERS)}.",
        )

    def _check_raw_string_list(
        self,
        node: list,
        path: str,
        parent_key: str | None,
        result: ValidationResult,
    ) -> None:
        """Flag a list of more than ``k`` raw strings under ANY key."""
        strings = [item for item in node if isinstance(item, str)]
        if len(strings) > self.max_raw_string_list:
            result.add(
                "raw-string-list",
                path,
                f"list under {parent_key!r} carries {len(strings)} raw strings "
                f"(> {self.max_raw_string_list}); a raw value dump "
                f"(e.g. top_values/samples) is a direct leak. Sample: "
                f"{strings[:3]!r}",
            )

    def _check_pii_string(
        self, value: str, path: str, result: ValidationResult
    ) -> None:
        """Flag any value matching a PII regex (SSN/email/phone/IP/IBAN)."""
        for label, rx in _PII_REGEXES.items():
            m = rx.search(value)
            if not m:
                continue
            if label == "phone":
                digits = sum(c.isdigit() for c in m.group(0))
                if not (_PHONE_MIN_DIGITS <= digits <= _PHONE_MAX_DIGITS):
                    continue
            result.add(
                "pii-regex",
                path,
                f"value matches {label} PII pattern: {m.group(0)!r}",
            )
            return  # one finding per leaf is enough

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_number(value: Any) -> bool:
        """True for a real numeric (int/float), excluding bool."""
        return isinstance(value, (int, float)) and not isinstance(value, bool)
