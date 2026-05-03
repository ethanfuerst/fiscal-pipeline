"""One-shot backfill for BTC historical prices.

Writes one parquet per UTC date to:
  s3://{BUCKET_NAME}/historical_btc_price/year=YYYY/month=MM/day=DD/data.parquet

Iterates from --start through today (UTC), fetching in chunks of up to 300
days from Coinbase Exchange. Skips any date whose partition file already
exists, so re-running is safe and resumable.

Run with op-resolved env (just for BUCKET_NAME and S3 creds — Coinbase needs
no auth):
  op run --env-file=.env -- uv run python scripts/backfill_btc_price.py
"""

import argparse
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd
from dotenv import load_dotenv
from eftoolkit.utils import setup_logging

from src.etl.etl import (
    BTC_PRICE_HISTORY_START_DATE,
    COINBASE_CANDLES_PER_CALL,
    COINBASE_REQUEST_DELAY_SECONDS,
    btc_price_partition_path,
    fetch_coinbase_btc_daily_candles,
)
from src.utils import get_s3

setup_logging()
load_dotenv()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--start',
        type=date.fromisoformat,
        default=BTC_PRICE_HISTORY_START_DATE,
        help=f'UTC date to start from (YYYY-MM-DD). Default {BTC_PRICE_HISTORY_START_DATE}.',
    )
    parser.add_argument(
        '--end',
        type=date.fromisoformat,
        default=None,
        help='UTC date to stop at, inclusive (YYYY-MM-DD). Default: today (UTC).',
    )
    parser.add_argument(
        '--overwrite',
        action='store_true',
        help='Re-fetch and overwrite even if a partition already exists.',
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not os.getenv('BUCKET_NAME'):
        raise SystemExit('BUCKET_NAME missing in env')

    s3 = get_s3()
    end_date = args.end or datetime.now(timezone.utc).date()
    if args.start > end_date:
        raise SystemExit(f'--start {args.start} is after end {end_date}; nothing to do')

    full_range = pd.date_range(start=args.start, end=end_date, freq='D').date
    needed_dates: set[date] = set()
    for day in full_range:
        if args.overwrite or not s3.file_exists(btc_price_partition_path(day)):
            needed_dates.add(day)

    skipped = len(full_range) - len(needed_dates)
    logging.info(
        f'Backfill range {args.start} → {end_date}: '
        f'{len(full_range)} day(s), {skipped} already present, '
        f'{len(needed_dates)} to fetch.'
    )
    if not needed_dates:
        return

    cursor = min(needed_dates)
    fetch_max = max(needed_dates)
    written = 0
    failed = 0
    while cursor <= fetch_max:
        chunk_end = min(
            cursor + timedelta(days=COINBASE_CANDLES_PER_CALL - 1), fetch_max
        )
        candles = fetch_coinbase_btc_daily_candles(cursor, chunk_end)
        for d, price in candles.items():
            if d not in needed_dates:
                continue
            df = pd.DataFrame([{'date': d.isoformat(), 'price_usd': price}])
            s3.write_df_to_parquet(df, btc_price_partition_path(d))
            written += 1

        # Anything we needed in this chunk but didn't get back from Coinbase
        # is unrecoverable for this run — log so the user notices.
        chunk_needed = {d for d in needed_dates if cursor <= d <= chunk_end}
        chunk_missing = chunk_needed - candles.keys()
        if chunk_missing:
            failed += len(chunk_missing)
            logging.warning(
                f'No Coinbase candle for {len(chunk_missing)} date(s) in '
                f'{cursor}→{chunk_end}: {sorted(chunk_missing)[:5]}…'
            )

        logging.info(
            f'Progress: written={written} failed={failed} '
            f'(of {len(needed_dates)} needed)'
        )
        cursor = chunk_end + timedelta(days=1)
        time.sleep(COINBASE_REQUEST_DELAY_SECONDS)

    logging.info(
        f'Backfill complete. written={written} skipped={skipped} '
        f'failed={failed} total_in_range={len(full_range)}'
    )


if __name__ == '__main__':
    main()
