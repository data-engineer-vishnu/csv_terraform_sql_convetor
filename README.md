# CSV to BigQuery Terraform and SQL Generator

This repository converts a metadata CSV that describes BigQuery table columns into:

- Terraform files for `google_bigquery_table`
- SQL DDL files for `CREATE TABLE`

The generator expects the CSV to contain these columns:

- `Dataset_name`
- `table_name`
- `column_name`
- `datatype`
- `required`
- `column_default_value`
- `primary_key`
- `foreign_key`
- `use_for_partition`
- `use_for_clustering`
- `clustering_order_num`

## Features

- Groups rows into BigQuery tables by dataset and table name
- Creates separate output folders under `terraform/` and `sql/` based on table suffixes:
  - `_THX` tables go into `thx/`
  - `_TEX` tables go into `tex/`
  - all others go into `common/`
- Uses Terraform heredoc `<<EOT` for the JSON schema
- Uses Terraform `project = var.project_id` on each table resource
- Uses `google_bigquery_dataset.dataset_<dataset_name>.dataset_id` for `dataset_id`
- Adds `PRIMARY KEY (...) NOT ENFORCED` when primary keys are marked
- Adds string foreign key references as `FOREIGN KEY (...) REFERENCES ... NOT ENFORCED`
- Supports one partitioning column per table
- Orders clustering columns by `clustering_order_num`

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
  common/
  tex/
  thx/
sql/
  common/
  tex/
  thx/
```

Each generated table gets:

- one Terraform file at `terraform/<layer>/<dataset>__<table>.tf`
- one SQL file at `sql/<layer>/<dataset>__<table>.sql`
- one shared Terraform variables file at `terraform/variables.tf`

## Notes

- Default values are written exactly as they appear in the CSV.
- Foreign keys are emitted only when the CSV cell is not empty.
- The foreign key reference is treated as a literal BigQuery reference string, for example `Sales.customers(customer_id)`.
- The generator normalizes boolean-style values like `TRUE`, `FALSE`, `yes`, `no`, `1`, and `0`.
- Terraform foreign-key references use `var.project_id`, so supply that variable in your Terraform root module or `tfvars`.
- Terraform table resources assume your dataset resources are defined with names like `google_bigquery_dataset.dataset_sales` for dataset `Sales`.
