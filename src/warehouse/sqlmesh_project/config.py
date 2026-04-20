import os

from sqlmesh.core.config import (
    Config,
    DuckDBConnectionConfig,
    GatewayConfig,
    ModelDefaultsConfig,
)

from src import project_root

config = Config(
    model_defaults=ModelDefaultsConfig(dialect='duckdb'),
    gateways={
        'duckdb': GatewayConfig(
            connection=DuckDBConnectionConfig(
                database=str(project_root / 'fiscal_pipeline.duckdb'),
                extensions=[
                    {'name': 'httpfs'},
                ],
                secrets=[
                    {
                        'type': 'S3',
                        'region': 'nyc3',
                        'endpoint': 'nyc3.digitaloceanspaces.com',
                        'key_id': os.getenv('S3_ACCESS_KEY_ID'),
                        'secret': os.getenv('S3_SECRET_ACCESS_KEY_ID'),
                    }
                ],
            )
        )
    },
)
