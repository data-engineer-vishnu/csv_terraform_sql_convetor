from __future__ import annotations

from pathlib import Path

from csv_bigquery_generator.parser import parse_csv
from csv_bigquery_generator.renderers import render_sql, render_terraform, write_outputs


def test_parse_csv_and_render_outputs(tmp_path: Path) -> None:
    sample_csv = Path("examples/sample_metadata.csv")
    tables = parse_csv(sample_csv)

    assert len(tables) == 6

    orders = next(table for table in tables if table.table_name == "orders")
    assert [column.name for column in orders.clustering_columns] == ["order_id", "customer_id"]
    assert orders.partition_column is not None
    assert orders.partition_column.name == "order_date"

    sql = render_sql(orders)
    assert "PRIMARY KEY (order_id) NOT ENFORCED" in sql
    assert "FOREIGN KEY (customer_id) REFERENCES Sales.customers(customer_id) NOT ENFORCED" in sql
    assert "CLUSTER BY order_id, customer_id" in sql

    terraform = render_terraform(orders, {(table.dataset_name, table.table_name): table for table in tables})
    assert 'resource "google_bigquery_table" "table_sales_orders"' in terraform
    assert "project    = var.project_id" in terraform
    assert "schema = <<EOT" in terraform
    assert "dataset_id = google_bigquery_dataset.dataset_sales.dataset_id" in terraform
    assert "depends_on = [google_bigquery_table.table_sales_customers]" in terraform
    assert '"defaultValueExpression": "\'PENDING\'"' in terraform
    assert "project_id = var.project_id" in terraform
    assert 'dataset_id = "Sales"' in terraform
    assert 'table_id   = "customers"' in terraform
    assert 'referenced_column  = "customer_id"' in terraform

    write_outputs(tables, tmp_path)

    assert (tmp_path / "terraform" / "variables.tf").exists()
    assert (tmp_path / "terraform" / "common" / "sales__orders.tf").exists()
    assert (tmp_path / "terraform" / "tex" / "sales__inventory_tex.tf").exists()
    assert (tmp_path / "terraform" / "thx" / "sales__customer_snapshot_thx.tf").exists()
    assert (tmp_path / "sql" / "common" / "sales__orders.sql").exists()
