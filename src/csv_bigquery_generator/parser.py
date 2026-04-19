from __future__ import annotations

import csv
from collections import OrderedDict
from pathlib import Path

from .models import Column, Table

EXPECTED_HEADERS = {
    "dataset_name": {
        "display": "DATASET_NAME",
        "aliases": {"dataset_name"},
    },
    "table_name": {
        "display": "TABLE_NAME",
        "aliases": {"table_name"},
    },
    "column_name": {
        "display": "COLUMN_NAME",
        "aliases": {"column_name"},
    },
    "ordinal_position": {
        "display": "ORDINAL_POSITION",
        "aliases": {"ordinal_position"},
    },
    "datatype": {
        "display": "DATATYPE",
        "aliases": {"datatype"},
    },
    "character_maximum_length": {
        "display": "CHARACTER_MAXIMUM_LENGTH",
        "aliases": {"character_maximum_length"},
    },
    "numeric_precision": {
        "display": "NUMERIC_PRESICION",
        "aliases": {"numeric_presicion", "numeric_precision"},
    },
    "numeric_scale": {
        "display": "NUMERIC_SCALE",
        "aliases": {"numeric_scale"},
    },
    "required": {
        "display": "REQUIRED",
        "aliases": {"required"},
    },
    "column_default": {
        "display": "COLUMN_DEFAULT",
        "aliases": {"column_default"},
    },
    "is_primary_key": {
        "display": "IS_PRIMARY_KEY",
        "aliases": {"is_primary_key"},
    },
    "is_foreign_key": {
        "display": "IS_FOREIGN_KEY",
        "aliases": {"is_foreign_key"},
    },
    "foreign_key_name": {
        "display": "FOREIGN_KEY_NAME",
        "aliases": {"foreign_key_name"},
    },
    "referenced_schema": {
        "display": "REFERENCED_SCHEMA",
        "aliases": {"referenced_schema"},
    },
    "referenced_table": {
        "display": "REFERENCED_TABLE",
        "aliases": {"referenced_table"},
    },
    "referenced_column": {
        "display": "REFERENCED_COLUMN",
        "aliases": {"referenced_column"},
    },
    "is_partition_key": {
        "display": "IS_PARTITION_KEY",
        "aliases": {"is_partition_key"},
    },
    "is_clustering_key": {
        "display": "IS_CLUSTERING_KEY",
        "aliases": {"is_clustering_key"},
    },
    "clustering_key_order_number": {
        "display": "CLUSTERING_KEY_ORDER_NUMBER",
        "aliases": {"clustering_key_order_number"},
    },
}


def parse_csv(csv_path: Path) -> list[Table]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        normalized_headers = {normalize_header(header): header for header in (reader.fieldnames or [])}
        headers = resolve_headers(normalized_headers)
        missing_headers = [header for header in EXPECTED_HEADERS if header not in headers]
        if missing_headers:
            missing_display = ", ".join(
                EXPECTED_HEADERS[header]["display"] for header in missing_headers
            )
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
                    ordinal_position=parse_required_int(
                        row.get(headers["ordinal_position"]),
                        headers["ordinal_position"],
                        row_number,
                    ),
                    data_type=data_type,
                    character_maximum_length=parse_optional_int(
                        row.get(headers["character_maximum_length"])
                    ),
                    numeric_precision=parse_optional_int(row.get(headers["numeric_precision"])),
                    numeric_scale=parse_optional_int(row.get(headers["numeric_scale"])),
                    required=parse_bool(row.get(headers["required"])),
                    default_value=clean_optional(row.get(headers["column_default"])),
                    is_primary_key=parse_bool(row.get(headers["is_primary_key"])),
                    is_foreign_key=parse_bool(row.get(headers["is_foreign_key"])),
                    foreign_key_name=clean_optional(row.get(headers["foreign_key_name"])),
                    referenced_schema=clean_optional(row.get(headers["referenced_schema"])),
                    referenced_table=clean_optional(row.get(headers["referenced_table"])),
                    referenced_column=clean_optional(row.get(headers["referenced_column"])),
                    use_for_partition=parse_bool(row.get(headers["is_partition_key"])),
                    use_for_clustering=parse_bool(row.get(headers["is_clustering_key"])),
                    clustering_order_num=parse_optional_int(
                        row.get(headers["clustering_key_order_number"])
                    ),
                )
            )

    for table in tables.values():
        table.columns.sort(key=lambda column: (column.ordinal_position, column.name))

    return list(tables.values())


def normalize_header(header: str | None) -> str:
    if header is None:
        return ""
    return header.strip().lower()


def resolve_headers(normalized_headers: dict[str, str]) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for internal_name, config in EXPECTED_HEADERS.items():
        for alias in config["aliases"]:
            if alias in normalized_headers:
                resolved[internal_name] = normalized_headers[alias]
                break
    return resolved


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


def parse_required_int(value: str | None, header: str, row_number: int) -> int:
    cleaned = clean_optional(value)
    if cleaned is None:
        raise ValueError(f"Row {row_number} is missing required value for '{header}'")
    return int(cleaned)
