from dataclasses import dataclass


@dataclass
class TableMetadata:
    source: str
    database_name: str
    table_name: str
    table_comment: str | None = None


@dataclass
class ColumnMetadata:
    source: str
    database_name: str
    table_name: str
    column_name: str
    data_type: str
    table_comment: str | None = None
    column_comment: str | None = None
    is_not_null: bool = True


@dataclass
class Metadata:
    tables: list[TableMetadata]
    columns: list[ColumnMetadata]
