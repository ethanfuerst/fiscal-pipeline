import os

from eftoolkit.s3 import S3FileSystem
from eftoolkit.sql import DuckDB

from src import project_root


def get_s3() -> S3FileSystem:
    return S3FileSystem(
        access_key_id=os.getenv('S3_ACCESS_KEY_ID'),
        secret_access_key=os.getenv('S3_SECRET_ACCESS_KEY_ID'),
        region='nyc3',
        endpoint='nyc3.digitaloceanspaces.com',
    )


def get_duckdb() -> DuckDB:
    return DuckDB(
        database=str(project_root / 'fiscal_pipeline.duckdb'),
        s3=get_s3(),
    )
