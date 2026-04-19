import os

import duckdb
from dotenv import load_dotenv

from src import project_root

load_dotenv()


class DuckDBConnection:
    def __init__(self):
        self.connection = duckdb.connect(
            database=project_root / 'fiscal_pipeline.duckdb', read_only=False
        )
        self._configure_connection()

    def _configure_connection(self):
        self.connection.execute(
            f"""
            install httpfs;
            load httpfs;
            CREATE OR REPLACE TEMPORARY SECRET s3_secret (
                TYPE S3,
                KEY_ID '{os.getenv('S3_ACCESS_KEY_ID')}',
                SECRET '{os.getenv('S3_SECRET_ACCESS_KEY_ID')}',
                REGION 'nyc3',
                ENDPOINT 'nyc3.digitaloceanspaces.com'
            );
            """
        )

    def get_connection(self):
        return self.connection

    def query(self, query):
        return self.connection.query(query)

    def execute(self, query, *args, **kwargs):
        self.connection.execute(query, *args, **kwargs)

    def close(self):
        self.connection.close()

    def df(self, query):
        return self.connection.query(query).df()
