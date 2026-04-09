from typing import override

import sqlalchemy

from scout.base_scout import DataScout
from scout.models import TableMetadata, ColumnMetadata

SAMPLE_SIZE = 10

class PostgresScout(DataScout):
    def __init__(self, url: str = None, user: str = None, password: str = None, host: str = None, port: str = None, database: str = None):
        self.url = url
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database

        if self.url is None:
            self.url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        self.engine = sqlalchemy.create_engine(self.url)

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
                    source="postgres",
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
                        source="postgres",
                        database_name=self.database,
                        table_name=table,
                        table_comment=table_comment,
                        column_name=c["name"],
                        data_type=str(c["type"]),
                        column_comment=c.get("comment"),
                        is_not_null=not c.get("nullable", True),
                    ))
        return result
