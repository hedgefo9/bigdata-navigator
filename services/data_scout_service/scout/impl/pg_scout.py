from typing import override
from urllib.parse import quote, urlsplit, urlunsplit

import sqlalchemy

from scout.base_scout import DataScout
from scout.models import TableMetadata, ColumnMetadata

SAMPLE_SIZE = 10

class PostgresScout(DataScout):
    def __init__(
        self,
        url: str = None,
        user: str = None,
        password: str = None,
        host: str = None,
        port: str = None,
        database: str = None,
        source: str = "postgres",
        source_name: str | None = None,
        source_kind: str | None = "sql",
        source_dialect: str | None = "postgresql",
        connection_uri: str | None = None,
        vault_url: str | None = None,
        vault_secret_ref: str | None = None,
    ):
        self.url = url
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.source = source
        self.source_name = source_name
        self.source_kind = source_kind
        self.source_dialect = source_dialect
        self.connection_uri = connection_uri
        self.vault_url = vault_url
        self.vault_secret_ref = vault_secret_ref

        if self.url is None:
            self.url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        self.engine = sqlalchemy.create_engine(self.url)
        if self.database is None:
            self.database = self.engine.url.database
        if self.connection_uri is None:
            self.connection_uri = self._strip_password(self.url)

    @staticmethod
    def _strip_password(uri: str | None) -> str | None:
        if uri is None:
            return None
        parsed = urlsplit(uri)
        if not parsed.username:
            return uri
        username = quote(parsed.username, safe="")
        auth = f"{username}@"
        if parsed.hostname:
            auth += parsed.hostname
        if parsed.port:
            auth += f":{parsed.port}"
        return urlunsplit((parsed.scheme, auth, parsed.path, parsed.query, parsed.fragment))

    @override
    def find_tables(self) -> list[TableMetadata]:
        inspect = sqlalchemy.inspect(self.engine)
        result = []
        for schema in inspect.get_schema_names():
            if schema.startswith("pg_") or schema == "information_schema":
                continue
            for table in inspect.get_table_names(schema=schema):
                comment = inspect.get_table_comment(table, schema=schema).get("text")
                result.append(TableMetadata(
                    source=self.source,
                    source_name=self.source_name,
                    source_kind=self.source_kind,
                    source_dialect=self.source_dialect,
                    connection_uri=self.connection_uri,
                    vault_url=self.vault_url,
                    vault_secret_ref=self.vault_secret_ref,
                    database_name=self.database,
                    table_name=table,
                    table_comment=comment
                ))
                # with (Session(engine) as session):
                #     query = f"SELECT * FROM {literal_column(schema)}.{literal_column(table)} LIMIT {SAMPLE_SIZE}"
                #     rows = session.execute(text(query)).fetchall()
                #     print(rows)
        return result

    @override
    def find_columns(self) -> list[ColumnMetadata]:
        inspect = sqlalchemy.inspect(self.engine)
        result = []
        for schema in inspect.get_schema_names():
            if schema.startswith("pg_") or schema == "information_schema":
                continue
            for table in inspect.get_table_names(schema=schema):
                table_comment = inspect.get_table_comment(table, schema=schema).get("text")
                for c in inspect.get_columns(table, schema=schema):
                    result.append(ColumnMetadata(
                        source=self.source,
                        source_name=self.source_name,
                        source_kind=self.source_kind,
                        source_dialect=self.source_dialect,
                        connection_uri=self.connection_uri,
                        vault_url=self.vault_url,
                        vault_secret_ref=self.vault_secret_ref,
                        database_name=self.database,
                        table_name=table,
                        table_comment=table_comment,
                        column_name=c["name"],
                        data_type=str(c["type"]),
                        column_comment=c.get("comment"),
                        is_not_null=not c.get("nullable", True),
                    ))
        return result
