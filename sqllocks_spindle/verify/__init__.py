# sqllocks_spindle/verify/__init__.py
"""Spindle verify — standalone quality and integrity verification."""

from sqllocks_spindle.verify.loader import load_tables

try:
    from sqllocks_spindle.verify.runner import VerifyResult, VerifyRunner
    from sqllocks_spindle.verify.report import VerifyReport
except ModuleNotFoundError:
    # runner and report are implemented in later tasks
    pass

__all__ = ["load_tables", "VerifyResult", "VerifyRunner", "VerifyReport"]
