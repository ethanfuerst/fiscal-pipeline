import logging
import shutil

from sqlmesh.core.context import Context

from src import project_root
from src.utils.logging_config import setup_logging

setup_logging()


def copy_duckdb_file() -> None:
    """Need to do this so evidence can read the file"""
    source_path = project_root / 'fiscal_pipeline.duckdb'
    dashboard_path = (
        project_root
        / 'dashboards'
        / 'sources'
        / 'fiscal_pipeline'
        / 'fiscal_pipeline.duckdb'
    )

    try:
        shutil.copy(source_path, dashboard_path)
        logging.info(f'Successfully copied {source_path} to {dashboard_path}')
    except Exception as e:
        logging.error(f'Failed to copy {source_path} to {dashboard_path}: {e}')


def create_data_warehouse(is_local_run: bool = True) -> None:
    sqlmesh_context = Context(
        paths=project_root / 'src' / 'warehouse' / 'sqlmesh_project'
    )
    plan = sqlmesh_context.plan()
    sqlmesh_context.apply(plan)
    _ = sqlmesh_context.run()

    if is_local_run:
        copy_duckdb_file()
