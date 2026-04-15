from __future__ import annotations

import csv
from collections import OrderedDict
from pathlib import Path

from .models import Column, Table

EXPECTED_HEADERS = {
    "dataset_name": "Dataset_name",
    "table_name": "table_name",
    "column_name": "column_name",
    "datatype": "datatype",
    "required": "required",
    "column_default_value": "column_default_value",
    "primary_key": "primary_key",
    "foreign_key": "foreign_key",
    "use_for_partition": "use_for_partition",
    "use_for_clustering": "use_for_clustering",
    "clustering_order_num": "clustering_order_num",
}


def parse_csv(csv_path: Path) -> list[Table]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        headers = {normalize_header(header): header for header in (reader.fieldnames or [])}
        missing_headers = [header for header in EXPECTED_HEADERS if header not in headers]
        if missing_headers:
            missing_display = ", ".join(EXPECTED_HEADERS[header] for header in missing_headers)
            raise ValueError(f"CSV is missing required columns: {missing_display}")

        tables: "OrderedDict[tuple[str, str], Table]" = OrderedDict()
        for row_number, row in enumerate(reader, start=2):
            dataset_name = required_value(row, headers["dataset_name"], row_number)
            table_name = required_value(row, headers["table_name"], row_number)
            column_name = required_value(row, headers["column_name"], row_number)
            data_type = required_value(row, headers["datatype"], row_number)

            key = (dataset_name, table_name)
            table = tables.setdefault(key, Table(dataset_name=dataset_name, table_name=table_name))

            table.columns.append(
                Column(
                    name=column_name,
                    data_type=data_type,
                    required=parse_bool(row.get(headers["required"])),
                    default_value=clean_optional(row.get(headers["column_default_value"])),
                    is_primary_key=parse_bool(row.get(headers["primary_key"])),
                    foreign_key=clean_optional(row.get(headers["foreign_key"])),
                    use_for_partition=parse_bool(row.get(headers["use_for_partition"])),
                    use_for_clustering=parse_bool(row.get(headers["use_for_clustering"])),
                    clustering_order_num=parse_optional_int(row.get(headers["clustering_order_num"])),
                )
            )

    return list(tables.values())


def normalize_header(header: str | None) -> str:
    if header is None:
        return ""
    return header.strip().lower()


def required_value(row: dict[str, str | None], header: str, row_number: int) -> str:
    value = clean_optional(row.get(header))
    if value is None:
        raise ValueError(f"Row {row_number} is missing required value for '{header}'")
    return value


def clean_optional(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def parse_bool(value: str | None) -> bool:
    cleaned = clean_optional(value)
    if cleaned is None:
        return False
    normalized = cleaned.lower()
    return normalized in {"true", "t", "yes", "y", "1"}


def parse_optional_int(value: str | None) -> int | None:
    cleaned = clean_optional(value)
    if cleaned is None:
        return None
    return int(cleaned)
