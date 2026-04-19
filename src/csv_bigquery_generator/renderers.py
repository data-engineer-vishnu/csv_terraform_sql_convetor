from __future__ import annotations

import json
import re
from pathlib import Path

from .models import Table


def write_outputs(tables: list[Table], output_root: Path) -> None:
    terraform_root = output_root / "terraform"
    sql_root = output_root / "sql"
    terraform_root.mkdir(parents=True, exist_ok=True)
    sql_root.mkdir(parents=True, exist_ok=True)
    clear_generated_layer_files(terraform_root)
    clear_generated_layer_files(sql_root)
    write_terraform_shared_files(terraform_root)
    table_index = build_table_index(tables)
    tables_by_layer = group_tables_by_layer(tables)

    for layer, layer_tables in tables_by_layer.items():
        terraform_dir = terraform_root / layer
        terraform_dir.mkdir(parents=True, exist_ok=True)
        terraform_content = "\n".join(
            render_terraform(table, table_index).rstrip() for table in layer_tables
        )
        (terraform_dir / f"{layer}.tf").write_text(f"{terraform_content}\n", encoding="utf-8")

    for table in tables:
        sql_dir = output_root / "sql" / table.layer
        sql_dir = output_root / "sql" / table.layer
        sql_dir.mkdir(parents=True, exist_ok=True)

        stem = f"{sanitize_name(table.dataset_name)}__{sanitize_name(table.table_name)}"
        (sql_dir / f"{stem}.sql").write_text(render_sql(table), encoding="utf-8")


def render_terraform(table: Table, table_index: dict[tuple[str, str], Table] | None = None) -> str:
    schema_objects = []
    for column in table.columns:
        schema_object = {
            "name": column.name,
            "type": column.data_type.upper(),
            "mode": "REQUIRED" if column.required else "NULLABLE",
        }
        if column.character_maximum_length is not None:
            schema_object["maxLength"] = str(column.character_maximum_length)
        if column.numeric_precision is not None:
            schema_object["precision"] = str(column.numeric_precision)
        if column.numeric_scale is not None:
            schema_object["scale"] = str(column.numeric_scale)
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
    return (
        f"resource \"google_bigquery_table\" \"{resource_name}\" {{\n"
        "  project    = var.project_id\n"
        f"  dataset_id = \"{table.dataset_name}\"\n"
        f"  table_id   = \"{table.table_name}\"\n"
        "  deletion_protection = false\n\n"
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
    if not table.primary_key_columns and not table.foreign_keys:
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
    for index, (foreign_key_name, columns) in enumerate(table.foreign_keys, start=1):
        reference = columns[0].foreign_key_reference
        if reference is None:
            continue
        lines.extend(
            [
                f"    foreign_keys {{",
                f"      name = \"{foreign_key_name or f'fk_{sanitize_name(table.table_name)}_{index}'}\"",
                "      referenced_table {",
                "        project_id = var.project_id",
                f"        dataset_id = \"{reference.referenced_schema}\"",
                f"        table_id   = \"{reference.referenced_table}\"",
                "      }",
            ]
        )
        for column in columns:
            if column.foreign_key_reference is None:
                continue
            lines.extend(
                [
                    "      column_references {",
                    f"        referencing_column = \"{column.name}\"",
                    f"        referenced_column  = \"{column.foreign_key_reference.referenced_column}\"",
                    "      }",
                ]
            )
        lines.append("    }")
    lines.append("  }")
    lines.append("")
    return "\n".join(lines)


def render_terraform_dependencies(
    table: Table, table_index: dict[tuple[str, str], Table]
) -> str:
    dependencies: list[str] = []
    for _, columns in table.foreign_keys:
        reference = columns[0].foreign_key_reference
        if reference is None:
            continue
        key = (reference.referenced_schema, reference.referenced_table)
        if key not in table_index:
            continue
        dependency_resource = sanitize_name(
            f"table_{reference.referenced_schema}_{reference.referenced_table}"
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
        line = f"  {column.name} {column.type_declaration}"
        if column.required:
            line += " NOT NULL"
        if column.default_value:
            line += f" DEFAULT {column.default_value}"
        column_lines.append(line)

    constraint_lines = []
    if table.primary_key_columns:
        pk_names = ", ".join(column.name for column in table.primary_key_columns)
        constraint_lines.append(f"  PRIMARY KEY ({pk_names}) NOT ENFORCED")
    for _, columns in table.foreign_keys:
        referencing_columns = ", ".join(column.name for column in columns)
        reference = columns[0].foreign_key_reference
        if reference is None:
            continue
        referenced_columns = ", ".join(
            column.foreign_key_reference.referenced_column
            for column in columns
            if column.foreign_key_reference is not None
        )
        constraint_name = (
            f"CONSTRAINT {columns[0].foreign_key_name} "
            if columns[0].foreign_key_name
            else ""
        )
        constraint_lines.append(
            "  "
            f"{constraint_name}FOREIGN KEY ({referencing_columns}) "
            f"REFERENCES {reference.referenced_schema}.{reference.referenced_table}"
            f"({referenced_columns}) NOT ENFORCED"
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


def group_tables_by_layer(tables: list[Table]) -> dict[str, list[Table]]:
    grouped: dict[str, list[Table]] = {"common": [], "tex": [], "thx": []}
    for table in tables:
        grouped.setdefault(table.layer, []).append(table)
    return grouped


def clear_generated_layer_files(root: Path) -> None:
    for layer in ("common", "tex", "thx"):
        layer_dir = root / layer
        if not layer_dir.exists():
            continue
        for generated_file in layer_dir.iterdir():
            if generated_file.is_file():
                generated_file.unlink()
