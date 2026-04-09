from dataclasses import dataclass


@dataclass
class TableMetadata:
    source: str
    database_name: str
    table_name: str
    table_comment: str


@dataclass
class ColumnMetadata:
    source: str
    database_name: str
    table_name: str
    table_comment: str
    column_name: str
    data_type: str
    column_comment: str
    is_not_null: bool = True


@dataclass
class Metadata:
    tables: list[TableMetadata]
    columns: list[ColumnMetadata]
