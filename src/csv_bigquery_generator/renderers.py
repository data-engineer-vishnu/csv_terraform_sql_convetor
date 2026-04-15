from __future__ import annotations

import json
import re
from pathlib import Path

from .models import Table


def write_outputs(tables: list[Table], output_root: Path) -> None:
    terraform_root = output_root / "terraform"
    terraform_root.mkdir(parents=True, exist_ok=True)
    write_terraform_shared_files(terraform_root)
    table_index = build_table_index(tables)

    for table in tables:
        terraform_dir = terraform_root / table.layer
        sql_dir = output_root / "sql" / table.layer
        terraform_dir.mkdir(parents=True, exist_ok=True)
        sql_dir.mkdir(parents=True, exist_ok=True)

        stem = f"{sanitize_name(table.dataset_name)}__{sanitize_name(table.table_name)}"
        (terraform_dir / f"{stem}.tf").write_text(
            render_terraform(table, table_index),
            encoding="utf-8",
        )
        (sql_dir / f"{stem}.sql").write_text(render_sql(table), encoding="utf-8")


def render_terraform(table: Table, table_index: dict[tuple[str, str], Table] | None = None) -> str:
    schema_objects = []
    for column in table.columns:
        schema_object = {
            "name": column.name,
            "type": column.data_type,
            "mode": "REQUIRED" if column.required else "NULLABLE",
        }
        if column.default_value:
            schema_object["defaultValueExpression"] = column.default_value
        schema_objects.append(schema_object)
    schema_json = json.dumps(schema_objects, indent=2)
    constraints_block = render_terraform_constraints(table)
    depends_on_block = render_terraform_dependencies(table, table_index or {})
    partition_block = ""
    if table.partition_column:
        partition_block = (
            "  time_partitioning {\n"
            f"    field = \"{table.partition_column.name}\"\n"
            "    type  = \"DAY\"\n"
            "  }\n\n"
        )
    clustering_block = ""
    if table.clustering_columns:
        columns = ", ".join(f"\"{column.name}\"" for column in table.clustering_columns)
        clustering_block = f"  clustering = [{columns}]\n\n"

    resource_name = sanitize_name(f"table_{table.dataset_name}_{table.table_name}")
    dataset_resource_name = sanitize_name(f"dataset_{table.dataset_name}")
    return (
        f"resource \"google_bigquery_table\" \"{resource_name}\" {{\n"
        "  project    = var.project_id\n"
        f"  dataset_id = google_bigquery_dataset.{dataset_resource_name}.dataset_id\n"
        f"  table_id   = \"{table.table_name}\"\n\n"
        f"{depends_on_block}"
        f"{partition_block}"
        f"{clustering_block}"
        "  schema = <<EOT\n"
        f"{schema_json}\n"
        "EOT\n"
        f"{constraints_block}"
        "}\n"
    )


def render_terraform_constraints(table: Table) -> str:
    if not table.primary_key_columns and not table.foreign_key_columns:
        return ""

    lines = ["", "  table_constraints {"]
    if table.primary_key_columns:
        pk_columns = ", ".join(f"\"{column.name}\"" for column in table.primary_key_columns)
        lines.extend(
            [
                "    primary_key {",
                f"      columns = [{pk_columns}]",
                "    }",
            ]
        )
    for index, column in enumerate(table.foreign_key_columns, start=1):
        reference = parse_reference(column.foreign_key)
        lines.extend(
            [
                f"    foreign_keys {{",
                f"      name = \"fk_{sanitize_name(table.table_name)}_{index}\"",
                "      referenced_table {",
                "        project_id = var.project_id",
                f"        dataset_id = \"{reference['dataset']}\"",
                f"        table_id   = \"{reference['table']}\"",
                "      }",
                "      column_references {",
                f"        referencing_column = \"{column.name}\"",
                f"        referenced_column  = \"{reference['column']}\"",
                "      }",
                "    }",
            ]
        )
    lines.append("  }")
    lines.append("")
    return "\n".join(lines)


def render_terraform_dependencies(
    table: Table, table_index: dict[tuple[str, str], Table]
) -> str:
    dependencies: list[str] = []
    for column in table.foreign_key_columns:
        reference = parse_reference(column.foreign_key)
        key = (reference["dataset"], reference["table"])
        if key not in table_index:
            continue
        dependency_resource = sanitize_name(
            f"table_{reference['dataset']}_{reference['table']}"
        )
        dependency = f"google_bigquery_table.{dependency_resource}"
        if dependency not in dependencies:
            dependencies.append(dependency)

    if not dependencies:
        return ""

    joined = ", ".join(dependencies)
    return f"  depends_on = [{joined}]\n\n"


def render_sql(table: Table) -> str:
    column_lines = []
    for column in table.columns:
        line = f"  {column.name} {column.data_type}"
        if column.required:
            line += " NOT NULL"
        if column.default_value:
            line += f" DEFAULT {column.default_value}"
        column_lines.append(line)

    constraint_lines = []
    if table.primary_key_columns:
        pk_names = ", ".join(column.name for column in table.primary_key_columns)
        constraint_lines.append(f"  PRIMARY KEY ({pk_names}) NOT ENFORCED")
    for column in table.foreign_key_columns:
        constraint_lines.append(
            f"  FOREIGN KEY ({column.name}) REFERENCES {format_sql_reference(column.foreign_key)} NOT ENFORCED"
        )

    body = ",\n".join(column_lines + constraint_lines)
    statement = (
        f"CREATE TABLE `{table.dataset_name}.{table.table_name}` (\n"
        f"{body}\n"
        ")"
    )

    options = []
    if table.partition_column:
        options.append(f"PARTITION BY {table.partition_column.name}")
    if table.clustering_columns:
        clustering = ", ".join(column.name for column in table.clustering_columns)
        options.append(f"CLUSTER BY {clustering}")
    if options:
        statement += "\n" + "\n".join(options)
    statement += ";\n"
    return statement


def sanitize_name(value: str) -> str:
    normalized = re.sub(r"[^0-9A-Za-z_]+", "_", value.strip())
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("_").lower()


def format_sql_reference(reference: str | None) -> str:
    if not reference:
        return ""
    parsed = parse_reference(reference)
    if parsed["column"]:
        return f"{parsed['dataset']}.{parsed['table']}({parsed['column']})"
    return f"{parsed['dataset']}.{parsed['table']}"


def extract_foreign_column(reference: str | None) -> str:
    if not reference:
        return ""
    match = re.search(r"\(([^)]+)\)", reference)
    if match:
        return match.group(1).strip()
    return ""


def parse_reference(reference: str | None) -> dict[str, str]:
    if not reference:
        return {"dataset": "", "table": "", "column": ""}

    cleaned = reference.strip()
    column = extract_foreign_column(cleaned)
    table_part = cleaned.split("(", 1)[0].strip()
    if "." not in table_part:
        return {"dataset": "", "table": table_part, "column": column}
    dataset, table = table_part.split(".", 1)
    return {"dataset": dataset.strip(), "table": table.strip(), "column": column}


def write_terraform_shared_files(terraform_root: Path) -> None:
    variables_tf = terraform_root / "variables.tf"
    variables_tf.write_text(
        'variable "project_id" {\n'
        '  description = "GCP project ID used for BigQuery tables and referenced tables."\n'
        '  type        = string\n'
        "}\n",
        encoding="utf-8",
    )


def build_table_index(tables: list[Table]) -> dict[tuple[str, str], Table]:
    return {(table.dataset_name, table.table_name): table for table in tables}
