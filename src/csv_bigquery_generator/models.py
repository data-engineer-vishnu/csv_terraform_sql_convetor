from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class Column:
    name: str
    data_type: str
    required: bool
    default_value: str | None = None
    is_primary_key: bool = False
    foreign_key: str | None = None
    use_for_partition: bool = False
    use_for_clustering: bool = False
    clustering_order_num: int | None = None


@dataclass(slots=True)
class Table:
    dataset_name: str
    table_name: str
    columns: list[Column] = field(default_factory=list)

    @property
    def partition_column(self) -> Column | None:
        partition_columns = [column for column in self.columns if column.use_for_partition]
        if not partition_columns:
            return None
        if len(partition_columns) > 1:
            names = ", ".join(column.name for column in partition_columns)
            raise ValueError(
                f"Table {self.dataset_name}.{self.table_name} has multiple partition columns: {names}"
            )
        return partition_columns[0]

    @property
    def clustering_columns(self) -> list[Column]:
        clustering_columns = [column for column in self.columns if column.use_for_clustering]
        return sorted(
            clustering_columns,
            key=lambda column: (
                column.clustering_order_num is None,
                column.clustering_order_num if column.clustering_order_num is not None else 10**9,
                column.name,
            ),
        )

    @property
    def primary_key_columns(self) -> list[Column]:
        return [column for column in self.columns if column.is_primary_key]

    @property
    def foreign_key_columns(self) -> list[Column]:
        return [column for column in self.columns if column.foreign_key]

    @property
    def layer(self) -> str:
        upper_name = self.table_name.upper()
        if upper_name.endswith("THX"):
            return "thx"
        if upper_name.endswith("TEX"):
            return "tex"
        return "common"
