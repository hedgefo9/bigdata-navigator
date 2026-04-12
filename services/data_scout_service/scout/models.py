from dataclasses import dataclass


@dataclass
class TableMetadata:
    source: str
    source_name: str | None
    source_kind: str | None
    source_dialect: str | None
    connection_uri: str | None
    vault_url: str | None
    vault_secret_ref: str | None
    database_name: str
    table_name: str
    table_comment: str | None = None


@dataclass
class ColumnMetadata:
    source: str
    source_name: str | None
    source_kind: str | None
    source_dialect: str | None
    connection_uri: str | None
    vault_url: str | None
    vault_secret_ref: str | None
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
