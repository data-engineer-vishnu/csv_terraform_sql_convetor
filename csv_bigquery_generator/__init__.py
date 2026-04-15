"""Compatibility package for running from the repository root."""

from __future__ import annotations

from pathlib import Path


_ROOT = Path(__file__).resolve().parents[1]
_SRC_PACKAGE = _ROOT / "src" / "csv_bigquery_generator"

if _SRC_PACKAGE.exists():
    __path__.append(str(_SRC_PACKAGE))
