# CSV to BigQuery Terraform and SQL Generator

This repository converts a metadata CSV that describes BigQuery table columns into:

- Terraform files for `google_bigquery_table`
- SQL DDL files for `CREATE TABLE`

The generator expects the CSV to contain these columns:

- `DATASET_NAME`
- `TABLE_NAME`
- `COLUMN_NAME`
- `ORDINAL_POSITION`
- `DATATYPE`
- `CHARACTER_MAXIMUM_LENGTH`
- `NUMERIC_PRESICION`
- `NUMERIC_SCALE`
- `REQUIRED`
- `COLUMN_DEFAULT`
- `IS_PRIMARY_KEY`
- `IS_FOREIGN_KEY`
- `FOREIGN_KEY_NAME`
- `REFERENCED_SCHEMA`
- `REFERENCED_TABLE`
- `REFERENCED_COLUMN`
- `IS_PARTITION_KEY`
- `IS_CLUSTERING_KEY`
- `CLUSTERING_KEY_ORDER_NUMBER`

## Features

- Groups rows into BigQuery tables by dataset and table name
- Preserves column order by `ORDINAL_POSITION`
- Creates separate output folders under `terraform/` and `sql/` based on table suffixes:
  - `_THX` tables go into `thx/`
  - `_TEX` tables go into `tex/`
  - all others go into `common/`
- Uses Terraform heredoc `<<EOT` for the JSON schema
- Uses Terraform `project = var.project_id` on each table resource
- Uses the CSV `DATASET_NAME` value directly for Terraform `dataset_id`
- Sets `deletion_protection = false` on generated BigQuery tables
- Supports single-column and composite primary keys
- Supports multiple foreign keys per table, including named foreign keys
- Carries `CHARACTER_MAXIMUM_LENGTH`, `NUMERIC_PRESICION`, and `NUMERIC_SCALE` into generated output
- Supports one partitioning column per table
- Orders clustering columns by `clustering_order_num`
- Clears previously generated files in `terraform/common|tex|thx` and `sql/common|tex|thx` before writing fresh output

## Install

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .[dev]
```

## Usage

```bash
csv-bq-gen --input examples/sample_metadata.csv --output .
```

Or run the module directly:

```bash
python -m csv_bigquery_generator --input examples/sample_metadata.csv --output .
```

## Output Structure

```text
terraform/
  common/common.tf
  tex/tex.tf
  thx/thx.tf
sql/
  common/
  tex/
  thx/
```

Each generated table gets:

- one consolidated Terraform file per layer at `terraform/<layer>/<layer>.tf`
- one SQL file at `sql/<layer>/<dataset>__<table>.sql`
- one shared Terraform variables file at `terraform/variables.tf`

## Notes

- Default values are written exactly as they appear in the CSV.
- Foreign keys are emitted only when `IS_FOREIGN_KEY` is true and the referenced schema, table, and column are present.
- `REFERENCED_SCHEMA` is treated as the BigQuery dataset name in generated SQL and Terraform.
- The generator normalizes boolean-style values like `TRUE`, `FALSE`, `yes`, `no`, `1`, and `0`.
- `NUMERIC_PRESICION` is intentionally spelled to match the incoming CSV contract.
- Terraform foreign-key references use `var.project_id`, so supply that variable in your Terraform root module or `tfvars`.
- Generated Terraform table resources work whether the BigQuery dataset already exists or is managed elsewhere, as long as `DATASET_NAME` matches the target dataset id.
- If a target dataset does not exist in BigQuery, Terraform will fail when creating that table until the dataset exists or the table is removed from the CSV input.
