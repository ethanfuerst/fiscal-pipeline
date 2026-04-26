import argparse
import logging

import modal
from eftoolkit.utils import setup_logging

from src.etl.etl import etl_ynab_data
from src.sheets.refresh_sheets import refresh_sheets
from src.warehouse.create_warehouse import create_data_warehouse

setup_logging()

app = modal.App('fiscal-pipeline')

modal_image = (
    modal.Image.debian_slim(python_version='3.10')
    .pip_install_from_pyproject('pyproject.toml')
    .add_local_dir(
        'src/warehouse/sqlmesh_project/',
        remote_path='/root/src/warehouse/sqlmesh_project/',
    )
    .add_local_python_source('src')
)


@app.function(
    image=modal_image,
    schedule=modal.Cron('5 8 * * *'),
    secrets=[modal.Secret.from_name('fiscal-pipeline-secrets')],
    retries=modal.Retries(
        max_retries=3,
        backoff_coefficient=1.0,
        initial_delay=60.0,
    ),
)
def update_google_sheet(
    sync_s3: bool = True,
    update_dashboards: bool = True,
    is_local_run: bool = False,
):
    if sync_s3:
        logging.info('Running S3 sync.')
        etl_ynab_data()
        logging.info('S3 sync completed.')

    if update_dashboards:
        logging.info('Updating dashboards.')
        create_data_warehouse(is_local_run=is_local_run)
        refresh_sheets()
        logging.info('Dashboard update process completed.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run the ETL process with specific parameters.'
    )
    parser.add_argument(
        '--sync-s3',
        action='store_true',
        help='Run the S3 sync process.',
    )
    parser.add_argument(
        '--update-dashboards',
        action='store_true',
        help='Run the dashboard update process.',
    )

    args = parser.parse_args()

    sync_s3 = args.sync_s3
    update_dashboards = args.update_dashboards

    logging.info('Running fiscal_pipeline_app locally')
    update_google_sheet.local(
        sync_s3=sync_s3,
        update_dashboards=update_dashboards,
        is_local_run=True,
    )
    logging.info('fiscal_pipeline_app completed')
