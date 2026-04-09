from scout.models import TableMetadata, ColumnMetadata, Metadata


class DataScout:

    def find_tables(self) -> list[TableMetadata]:
        pass

    def find_columns(self) -> list[ColumnMetadata]:
        pass

    def get_metadata(self) -> Metadata:
        return Metadata(self.find_tables(), self.find_columns())
