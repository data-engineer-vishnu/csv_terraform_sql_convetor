from __future__ import annotations

from pathlib import Path

from csv_bigquery_generator.parser import parse_csv
from csv_bigquery_generator.renderers import render_sql, render_terraform, write_outputs


def test_parse_csv_and_render_outputs(tmp_path: Path) -> None:
    sample_csv = Path("examples/sample_metadata.csv")
    tables = parse_csv(sample_csv)

    assert len(tables) == 9

    orders = next(table for table in tables if table.table_name == "orders")
    assert [column.name for column in orders.clustering_columns] == ["order_id", "customer_id"]
    assert orders.partition_column is not None
    assert orders.partition_column.name == "order_date"
    assert [column.ordinal_position for column in orders.columns] == [1, 2, 3, 4, 5]

    sql = render_sql(orders)
    assert "PRIMARY KEY (order_id) NOT ENFORCED" in sql
    assert (
        "CONSTRAINT FK_ORDERS_CUSTOMER FOREIGN KEY (customer_id) "
        "REFERENCES Sales.customers(customer_id) NOT ENFORCED"
    ) in sql
    assert "CLUSTER BY order_id, customer_id" in sql
    assert "total_amount NUMERIC(14, 2) NOT NULL DEFAULT 0" in sql

    terraform = render_terraform(orders, {(table.dataset_name, table.table_name): table for table in tables})
    assert 'resource "google_bigquery_table" "table_sales_orders"' in terraform
    assert "project    = var.project_id" in terraform
    assert "schema = <<EOT" in terraform
    assert 'dataset_id = "Sales"' in terraform
    assert "deletion_protection = false" in terraform
    assert "depends_on = [google_bigquery_table.table_sales_customers]" in terraform
    assert '"defaultValueExpression": "\'PENDING\'"' in terraform
    assert "project_id = var.project_id" in terraform
    assert 'dataset_id = "Sales"' in terraform
    assert 'table_id   = "customers"' in terraform
    assert 'referenced_column  = "customer_id"' in terraform
    assert '"precision": "14"' in terraform
    assert '"scale": "2"' in terraform
    assert '"maxLength": "30"' in terraform

    order_items = next(table for table in tables if table.table_name == "order_items")
    order_items_sql = render_sql(order_items)
    assert "PRIMARY KEY (order_id, line_number) NOT ENFORCED" in order_items_sql
    assert (
        "CONSTRAINT FK_ORDER_ITEMS_ORDER FOREIGN KEY (order_id) "
        "REFERENCES Sales.orders(order_id) NOT ENFORCED"
    ) in order_items_sql
    assert (
        "CONSTRAINT FK_ORDER_ITEMS_PRODUCT FOREIGN KEY (product_id) "
        "REFERENCES Sales.products(product_id) NOT ENFORCED"
    ) in order_items_sql

    ledger_entries = next(table for table in tables if table.table_name == "ledger_entries")
    ledger_sql = render_sql(ledger_entries)
    assert "PRIMARY KEY (entry_id, account_id) NOT ENFORCED" in ledger_sql
    assert "PARTITION BY entry_date" in ledger_sql
    assert "CLUSTER BY currency_code, amount" in ledger_sql
    assert "amount NUMERIC(18, 4) NOT NULL" in ledger_sql

    write_outputs(tables, tmp_path)

    assert (tmp_path / "terraform" / "variables.tf").exists()
    assert (tmp_path / "terraform" / "common" / "common.tf").exists()
    assert (tmp_path / "terraform" / "tex" / "tex.tf").exists()
    assert (tmp_path / "terraform" / "thx" / "thx.tf").exists()
    assert (tmp_path / "sql" / "common" / "sales__orders.sql").exists()
    common_tf = (tmp_path / "terraform" / "common" / "common.tf").read_text(encoding="utf-8")
    assert 'resource "google_bigquery_table" "table_sales_orders"' in common_tf
    assert 'resource "google_bigquery_table" "table_finance_ledger_entries"' in common_tf
    tex_tf = (tmp_path / "terraform" / "tex" / "tex.tf").read_text(encoding="utf-8")
    assert 'resource "google_bigquery_table" "table_sales_inventory_tex"' in tex_tf
    thx_tf = (tmp_path / "terraform" / "thx" / "thx.tf").read_text(encoding="utf-8")
    assert 'resource "google_bigquery_table" "table_sales_customer_snapshot_thx"' in thx_tf

    stale_terraform = tmp_path / "terraform" / "common" / "stale.tf"
    stale_sql = tmp_path / "sql" / "common" / "stale.sql"
    stale_terraform.write_text("stale", encoding="utf-8")
    stale_sql.write_text("stale", encoding="utf-8")

    write_outputs(tables, tmp_path)

    assert not stale_terraform.exists()
    assert not stale_sql.exists()
