import json
import schedule
import sqlalchemy
import yaml

from scout.impl.clickhouse_scout import ClickhouseScout
from scout.impl.pg_scout import PostgresScout
from scout.models import Metadata
from utils import parse_source


class App:
    def __init__(self, config="./config/config.yaml"):
        with open(config, 'r') as file:
            config_data = yaml.safe_load(file)
        username = config_data["datasource"]["main"]["username"]
        password = config_data["datasource"]["main"]["password"]
        host = config_data["datasource"]["main"]["host"]
        port = config_data["datasource"]["main"]["port"]
        database = config_data["datasource"]["main"]["database"]
        url = f"postgresql://{username}:{password}@{host}:{port}/{database}"

        self.engine = sqlalchemy.create_engine(url)

    def send_metadata(self, metadata: Metadata):
        pass


    def walk_sources(self):
        sources = None
        with open("config/sources.json", "r") as f:
            sources = json.load(f)
        for source in sources:
            source_type = source.split(":")[0]
            match source_type:
                case "postgresql":
                    pg_scout = PostgresScout(source)
                    metadata = pg_scout.get_metadata()
                    self.send_metadata(metadata)
                case "clickhouse":
                    source_params = parse_source(source)
                    source_params.pop("scheme")
                    ch_scout = ClickhouseScout(**source_params)
                    metadata = ch_scout.get_metadata()
                    self.send_metadata(metadata)


if __name__ == "__main__":
    app = App()
    schedule.every(10).seconds.do(app.walk_sources)

    while True:
        schedule.run_pending()
