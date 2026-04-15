from __future__ import annotations

import argparse
from pathlib import Path

from .parser import parse_csv
from .renderers import write_outputs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate BigQuery Terraform and SQL files from metadata CSV."
    )
    parser.add_argument("--input", required=True, type=Path, help="Path to the input CSV metadata file.")
    parser.add_argument(
        "--output",
        default=Path("."),
        type=Path,
        help="Directory where terraform/ and sql/ folders will be generated.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    tables = parse_csv(args.input)
    write_outputs(tables, args.output)
    print(
        f"Generated {len(tables)} tables under '{args.output / 'terraform'}' and '{args.output / 'sql'}'."
    )
    return 0
