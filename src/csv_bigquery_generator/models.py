from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class ForeignKeyReference:
    name: str | None
    referenced_schema: str
    referenced_table: str
    referenced_column: str


@dataclass(slots=True)
class Column:
    name: str
    ordinal_position: int
    data_type: str
    required: bool
    character_maximum_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None
    default_value: str | None = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_name: str | None = None
    referenced_schema: str | None = None
    referenced_table: str | None = None
    referenced_column: str | None = None
    use_for_partition: bool = False
    use_for_clustering: bool = False
    clustering_order_num: int | None = None

    @property
    def type_declaration(self) -> str:
        data_type = self.data_type.upper()
        if self.character_maximum_length is not None and data_type in {"STRING", "BYTES"}:
            return f"{data_type}({self.character_maximum_length})"
        if (
            self.numeric_precision is not None
            and data_type in {"NUMERIC", "BIGNUMERIC", "DECIMAL", "BIGDECIMAL"}
        ):
            if self.numeric_scale is not None:
                return f"{data_type}({self.numeric_precision}, {self.numeric_scale})"
            return f"{data_type}({self.numeric_precision})"
        return data_type

    @property
    def foreign_key_reference(self) -> ForeignKeyReference | None:
        if not self.is_foreign_key:
            return None
        if not (self.referenced_schema and self.referenced_table and self.referenced_column):
            return None
        return ForeignKeyReference(
            name=self.foreign_key_name,
            referenced_schema=self.referenced_schema,
            referenced_table=self.referenced_table,
            referenced_column=self.referenced_column,
        )


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
    def foreign_keys(self) -> list[tuple[str | None, list[Column]]]:
        grouped: dict[str, list[Column]] = {}
        ordered_keys: list[str] = []

        for column in self.columns:
            if not column.is_foreign_key:
                continue

            group_key = (
                column.foreign_key_name
                or f"{column.referenced_schema or ''}.{column.referenced_table or ''}"
            )
            if group_key not in grouped:
                grouped[group_key] = []
                ordered_keys.append(group_key)
            grouped[group_key].append(column)

        return [
            (grouped[group_key][0].foreign_key_name, grouped[group_key])
            for group_key in ordered_keys
        ]

    @property
    def layer(self) -> str:
        upper_name = self.table_name.upper()
        if upper_name.endswith("THX"):
            return "thx"
        if upper_name.endswith("TEX"):
            return "tex"
        return "common"
