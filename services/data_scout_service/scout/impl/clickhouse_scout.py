from typing import override

import clickhouse_connect

from scout.base_scout import DataScout
from scout.models import TableMetadata, ColumnMetadata


class ClickhouseScout(DataScout):
    def __init__(
            self,
            user: str = None,
            password: str = None,
            host: str = None,
            port: str = None,
            database: str = None,
            source: str = "clickhouse",
            source_name: str | None = None,
            source_kind: str | None = "sql",
            source_dialect: str | None = "clickhouse",
            connection_uri: str | None = None,
            vault_url: str | None = None,
            vault_secret_ref: str | None = None,
    ):
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

        self.client = clickhouse_connect.get_client(
            host=host, port=int(port), database=database, username=user, password=password
        )

    @override
    def find_tables(self) -> list[TableMetadata]:
        database = self.database or self.client.database
        query = """
                SELECT name    AS table_name,
                       comment AS table_comment
                FROM system.tables
                WHERE database = %(database)s
                  AND engine != 'View'
                ORDER BY table_name \
                """
        query_result = self.client.query(query, parameters={"database": database})
        return [
            TableMetadata(
                source=self.source,
                source_name=self.source_name,
                source_kind=self.source_kind,
                source_dialect=self.source_dialect,
                connection_uri=self.connection_uri,
                vault_url=self.vault_url,
                vault_secret_ref=self.vault_secret_ref,
                database_name=database,
                table_name=row["table_name"],
                table_comment=row["table_comment"],
            )
            for row in query_result.named_results()
        ]

    @override
    def find_columns(self) -> list[ColumnMetadata]:
        database = self.database or self.client.database
        query = """
                SELECT c.table                                     AS table_name,
                       t.comment                                   AS table_comment,
                       c.name                                      AS column_name,
                       c.type                                      AS data_type,
                       c.comment                                   AS column_comment,
                       toBool(NOT startsWith(c.type, 'Nullable(')) AS is_not_null
                FROM system.columns AS c
                         INNER JOIN system.tables AS t
                                    ON t.database = c.database
                                        AND t.name = c.table
                WHERE c.database = %(database)s
                  AND t.engine != 'View'
                ORDER BY table_name, c.position \
                """
        query_result = self.client.query(query, parameters={"database": database})
        return [
            ColumnMetadata(
                source=self.source,
                source_name=self.source_name,
                source_kind=self.source_kind,
                source_dialect=self.source_dialect,
                connection_uri=self.connection_uri,
                vault_url=self.vault_url,
                vault_secret_ref=self.vault_secret_ref,
                database_name=database,
                table_name=row["table_name"],
                table_comment=row["table_comment"],
                column_name=row["column_name"],
                data_type=row["data_type"],
                column_comment=row["column_comment"],
                is_not_null=bool(row["is_not_null"]),
            )
            for row in query_result.named_results()
        ]
