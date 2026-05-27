import json
import logging
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import yfinance as yf
from dotenv import load_dotenv
from eftoolkit.gsheets import Spreadsheet
from eftoolkit.s3 import S3FileSystem
from eftoolkit.utils import setup_logging

from src.utils import get_s3

setup_logging()
load_dotenv()

BUCKET_NAME = os.getenv('BUCKET_NAME')


ESPLORA_BASE_URL = 'https://blockstream.info/api'
ESPLORA_REQUEST_DELAY_SECONDS = 0.1
# Coinbase Exchange public market-data API; no auth, ~10 req/sec limit, max
# 300 candles per response, full BTC-USD history back to 2015.
COINBASE_BASE_URL = 'https://api.exchange.coinbase.com'
COINBASE_REQUEST_DELAY_SECONDS = 0.15
COINBASE_CANDLES_PER_CALL = 300
HISTORICAL_BTC_PRICE_PREFIX = 'historical_btc_price'
BTC_PRICE_HISTORY_START_DATE = date(2021, 6, 1)
BTC_PRICE_REFRESH_TAIL_DAYS = 7
_BTC_PRICE_PARTITION_RE = re.compile(r'year=(\d{4})/month=(\d{2})/day=(\d{2})/')
TICKER_PRICES_REFRESH_TAIL_DAYS = 7


def extract_budget_data() -> Dict:
    logging.info('Extracting budget data')

    budget_id = os.getenv('BUDGET_ID')
    bearer_token = os.getenv('BEARER_TOKEN')
    response = requests.get(
        f'https://api.ynab.com/v1/budgets/{budget_id}',
        headers={'Authorization': f'Bearer {bearer_token}'},
    )

    logging.info('Extracted budget data')

    return response.json()['data']['budget']


def extract_category_groups(budget_data: Dict, s3: S3FileSystem) -> None:
    df = pd.DataFrame(budget_data['category_groups']).reset_index(drop=True)
    s3.write_df_to_parquet(df, f's3://{BUCKET_NAME}/category-groups.parquet')


def extract_categories(budget_data: Dict, s3: S3FileSystem) -> None:
    dfs = []
    for month in budget_data['months']:
        month_date = datetime.strptime(month['month'], '%Y-%m-%d')
        monthly = pd.DataFrame(month['categories'])
        if monthly.empty:
            continue
        monthly['year'] = month_date.year
        monthly['month'] = month_date.month
        dfs.append(monthly)

    df = pd.concat(dfs).reset_index(drop=True)
    s3.write_df_to_parquet(df, f's3://{BUCKET_NAME}/monthly-categories.parquet')


def extract_transactions(budget_data: Dict, s3: S3FileSystem) -> None:
    df = pd.DataFrame(budget_data['transactions']).reset_index(drop=True)
    s3.write_df_to_parquet(df, f's3://{BUCKET_NAME}/transactions.parquet')


def extract_subtransactions(budget_data: Dict, s3: S3FileSystem) -> None:
    df = pd.DataFrame(budget_data['subtransactions']).reset_index(drop=True)
    s3.write_df_to_parquet(df, f's3://{BUCKET_NAME}/subtransactions.parquet')


def extract_accounts(budget_data: Dict, s3: S3FileSystem) -> None:
    df = pd.DataFrame(budget_data['accounts']).reset_index(drop=True)
    # YNAB returns debt_* fields as dicts keyed by date; for non-debt accounts
    # they're empty {}, which pyarrow can't write as a struct with no children.
    # Serialize any dict-valued column to a JSON string to keep the schema stable.
    for col in df.columns:
        if df[col].apply(lambda v: isinstance(v, dict)).any():
            df[col] = df[col].apply(
                lambda v: json.dumps(v) if isinstance(v, dict) else v
            )
    s3.write_df_to_parquet(df, f's3://{BUCKET_NAME}/accounts.parquet')


def load_paystubs_from_sheets(s3: S3FileSystem) -> None:
    credentials = json.loads(os.getenv('GSPREAD_CREDENTIALS').replace('\n', '\\n'))
    with Spreadsheet(credentials=credentials, spreadsheet_name='Paystubs') as ss:
        df = ss.worksheet('all_paystubs').read(dtype=str)

    df = df.reset_index(drop=True)
    s3.write_df_to_parquet(df, f's3://{BUCKET_NAME}/raw-paystubs.parquet')


def load_annual_contributions_from_sheets(s3: S3FileSystem) -> None:
    credentials = json.loads(os.getenv('GSPREAD_CREDENTIALS').replace('\n', '\\n'))
    with Spreadsheet(credentials=credentials, spreadsheet_name='Paystubs') as ss:
        df = ss.worksheet('annual_contributions').read(dtype=str)

    df = df.reset_index(drop=True)
    s3.write_df_to_parquet(df, f's3://{BUCKET_NAME}/raw-annual-contributions.parquet')


def load_investment_transactions_from_sheets(s3: S3FileSystem) -> None:
    credentials = json.loads(os.getenv('GSPREAD_CREDENTIALS').replace('\n', '\\n'))
    with Spreadsheet(
        credentials=credentials, spreadsheet_name='Investment Transactions'
    ) as ss:
        df = ss.worksheet('transactions').read(dtype=str)

    df = df.reset_index(drop=True)
    s3.write_df_to_parquet(
        df, f's3://{BUCKET_NAME}/raw-investment-transactions.parquet'
    )


def fetch_esplora_address_txs(
    address: str, last_seen_txid: Optional[str] = None
) -> List[Dict]:
    if last_seen_txid is None:
        url = f'{ESPLORA_BASE_URL}/address/{address}/txs'
    else:
        url = f'{ESPLORA_BASE_URL}/address/{address}/txs/chain/{last_seen_txid}'
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_all_wallet_rows(wallet_address: str) -> List[Dict]:
    # Minimal transformation: flatten status struct, keep vin/vout as JSON.
    # Per-wallet attribution (received/sent) lives in the cleaned model.
    rows: List[Dict] = []
    last_seen_txid: Optional[str] = None
    while True:
        batch = fetch_esplora_address_txs(wallet_address, last_seen_txid)
        if not batch:
            break

        confirmed_in_batch = [
            tx for tx in batch if (tx.get('status') or {}).get('confirmed')
        ]
        if not confirmed_in_batch:
            break

        for tx in confirmed_in_batch:
            status = tx.get('status') or {}
            rows.append(
                {
                    'wallet_address': wallet_address,
                    'txid': tx['txid'],
                    'fee': tx.get('fee'),
                    'size': tx.get('size'),
                    'weight': tx.get('weight'),
                    'block_height': status.get('block_height'),
                    'block_time': status.get('block_time'),
                    'confirmed': bool(status.get('confirmed')),
                    'vin_json': json.dumps(tx.get('vin', [])),
                    'vout_json': json.dumps(tx.get('vout', [])),
                }
            )

        last_seen_txid = confirmed_in_batch[-1]['txid']
        time.sleep(ESPLORA_REQUEST_DELAY_SECONDS)

    return rows


def extract_btc_wallet_history(s3: S3FileSystem) -> None:
    address_list_raw = os.getenv('BTC_ADDRESS_LIST', '')
    wallet_addresses = [a.strip() for a in address_list_raw.split(',') if a.strip()]
    if not wallet_addresses:
        logging.warning('BTC_ADDRESS_LIST is empty; skipping BTC wallet history.')
        return

    logging.info(f'Extracting BTC wallet history for {len(wallet_addresses)} wallet(s)')
    rows: List[Dict] = []
    for wallet_address in wallet_addresses:
        wallet_rows = fetch_all_wallet_rows(wallet_address)
        logging.info(
            f'Fetched {len(wallet_rows)} tx(s) for wallet '
            f'{wallet_address[:8]}…{wallet_address[-4:]}'
        )
        rows.extend(wallet_rows)

    if not rows:
        logging.info('No BTC wallet rows fetched; skipping write.')
        return

    df = pd.DataFrame(rows)
    s3.write_df_to_parquet(df, f's3://{BUCKET_NAME}/btc-wallet-history.parquet')


def btc_price_partition_path(day: date) -> str:
    return (
        f's3://{BUCKET_NAME}/{HISTORICAL_BTC_PRICE_PREFIX}/'
        f'year={day.year}/month={day.month:02d}/day={day.day:02d}/data.parquet'
    )


def collect_existing_btc_price_dates(s3: S3FileSystem) -> set:
    """List existing day-level partitions and parse the date from each path.

    Reading parquet content would be ~1500 GETs on first run; the path itself
    encodes the date, so we just parse it.
    """
    dates: set = set()
    prefix = f's3://{BUCKET_NAME}/{HISTORICAL_BTC_PRICE_PREFIX}/'
    for obj in s3.ls(prefix):
        if not obj.uri.endswith('.parquet'):
            continue
        m = _BTC_PRICE_PARTITION_RE.search(obj.uri)
        if m:
            dates.add(date(int(m[1]), int(m[2]), int(m[3])))
    return dates


def fetch_coinbase_btc_daily_candles(
    start_day: date, end_day: date
) -> Dict[date, float]:
    """Fetch BTC-USD daily close prices from Coinbase Exchange.

    Args:
        start_day: first UTC date to include (inclusive).
        end_day: last UTC date to include (inclusive).

    Returns dict {utc_date: close_usd}. Caller chunks longer ranges to stay
    under Coinbase's 300-candle-per-response limit.
    """
    start_iso = datetime.combine(
        start_day, datetime.min.time(), tzinfo=timezone.utc
    ).isoformat()
    # Coinbase uses [start, end) semantics — bump end by one day to include end_day.
    end_iso = datetime.combine(
        end_day + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc
    ).isoformat()
    response = requests.get(
        f'{COINBASE_BASE_URL}/products/BTC-USD/candles',
        params={'granularity': 86400, 'start': start_iso, 'end': end_iso},
        timeout=30,
    )
    if not response.ok:
        raise requests.HTTPError(
            f'{response.status_code} from Coinbase /products/BTC-USD/candles '
            f'start={start_iso} end={end_iso}: {response.text[:500]}',
            response=response,
        )
    # Coinbase returns [timestamp, low, high, open, close, volume] per candle.
    return {
        datetime.fromtimestamp(c[0], tz=timezone.utc).date(): float(c[4])
        for c in response.json()
    }


def extract_btc_price_history(s3: S3FileSystem) -> None:
    today = datetime.now(timezone.utc).date()
    start_date = BTC_PRICE_HISTORY_START_DATE
    if today < start_date:
        return

    existing_dates = collect_existing_btc_price_dates(s3)
    full_range = set(pd.date_range(start=start_date, end=today, freq='D').date)
    missing_dates = full_range - existing_dates

    refresh_window_start = max(
        start_date, today - timedelta(days=BTC_PRICE_REFRESH_TAIL_DAYS - 1)
    )
    refresh_dates = set(
        pd.date_range(start=refresh_window_start, end=today, freq='D').date
    )

    dates_to_fetch = missing_dates | refresh_dates
    if not dates_to_fetch:
        logging.info('BTC price history already current; nothing to fetch.')
        return

    logging.info(
        f'Fetching BTC prices for {len(dates_to_fetch)} date(s) '
        f'({len(missing_dates)} missing + {len(refresh_dates)} refresh-tail).'
    )

    # Walk [min, max] in 300-day chunks; filter response to the dates we wanted.
    # Non-contiguous gaps are inefficient (extra candles dropped) but rare.
    date_min = min(dates_to_fetch)
    date_max = max(dates_to_fetch)
    fetched: Dict[date, float] = {}
    cursor = date_min
    while cursor <= date_max:
        chunk_end = min(
            cursor + timedelta(days=COINBASE_CANDLES_PER_CALL - 1), date_max
        )
        candles = fetch_coinbase_btc_daily_candles(cursor, chunk_end)
        for d, price in candles.items():
            if d in dates_to_fetch:
                fetched[d] = price
        cursor = chunk_end + timedelta(days=1)
        time.sleep(COINBASE_REQUEST_DELAY_SECONDS)

    if not fetched:
        logging.info('No BTC price rows written (Coinbase returned no candles).')
        return

    for day in sorted(fetched):
        df_one = pd.DataFrame([{'date': day.isoformat(), 'price_usd': fetched[day]}])
        s3.write_df_to_parquet(df_one, btc_price_partition_path(day))
    logging.info(f'Wrote {len(fetched)} BTC price partition(s).')


def get_ticker_price_universe(s3: S3FileSystem) -> List[Tuple[str, date]]:
    """Distinct (ticker, earliest_trade_date) pairs from the investment transactions parquet.

    Filters out empty symbols (cash-only events) and multi-word fund names that don't have
    a real yfinance ticker (e.g. ``TRP RETIRE 2065 F``). Tickers that yfinance has no
    data for (money-market funds like FDRXX/SPAXX, exotic instruments, typos) flow
    through and get a $1 NAV default at fetch time — see ``extract_ticker_prices``.
    """
    path = f's3://{BUCKET_NAME}/raw-investment-transactions.parquet'
    if not s3.file_exists(path):
        return []
    df = s3.read_df_from_parquet(path)
    if df.empty:
        return []
    df = df[df['symbol'].notna()]
    df = df[df['symbol'].astype(str).str.strip() != '']
    df = df[~df['symbol'].astype(str).str.contains(' ', regex=False, na=False)]
    if df.empty:
        return []
    df = df.assign(
        _trade_date_parsed=pd.to_datetime(df['trade_date'], errors='coerce').dt.date
    )
    df = df[df['_trade_date_parsed'].notna()]
    earliest = df.groupby('symbol')['_trade_date_parsed'].min()
    return sorted(earliest.items())


def fetch_yfinance_ticker_history(ticker: str, start: date, end: date) -> pd.DataFrame:
    """Fetch OHLCV + adjusted close from yfinance for a given ticker and date range.

    Returns a DataFrame with columns
    ``date, open, high, low, close, adj_close, volume`` (date as ISO string).
    Empty DataFrame with those columns if yfinance returns nothing.
    """
    cols = ['date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']
    yf_ticker = yf.Ticker(ticker)
    # yfinance treats end as exclusive — bump by a day to include `end`.
    hist = yf_ticker.history(
        start=start, end=end + timedelta(days=1), auto_adjust=False
    )
    if hist is None or hist.empty:
        return pd.DataFrame(columns=cols)
    hist = hist.reset_index()
    return pd.DataFrame(
        {
            'date': pd.to_datetime(hist['Date']).dt.strftime('%Y-%m-%d'),
            'open': hist['Open'],
            'high': hist['High'],
            'low': hist['Low'],
            'close': hist['Close'],
            'adj_close': hist['Adj Close'],
            'volume': hist['Volume'],
        }
    )


def extract_ticker_prices(s3: S3FileSystem) -> None:
    """Backfill a per-ticker parquet of daily OHLCV + adj-close for every ticker
    that has appeared in the investment transactions ledger.

    One parquet file per ticker at ``s3://{bucket}/ticker_prices/ticker={X}/data.parquet``.
    Each refresh reads the existing file, set-diffs missing business days against
    [earliest_trade_date, today], force-includes a 7-business-day refresh tail (for
    yfinance back-corrections), fetches `[min(missing), max(missing)]` in a single
    yfinance call, merges (de-dup by date, keep last), and rewrites.

    Tickers yfinance has no data for (money-market funds, exotic instruments, typos)
    produce no rows — the partition simply doesn't exist. Downstream reconstruction
    models source prices from cleaned.investment_transactions for absent (ticker, date)
    pairs.
    """
    today = datetime.now(timezone.utc).date()
    universe = get_ticker_price_universe(s3)
    if not universe:
        logging.info(
            'No tickers to backfill (investment transactions parquet missing or empty).'
        )
        return

    logging.info(f'Backfilling ticker prices for {len(universe)} ticker(s).')

    cols = ['date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']

    for ticker, earliest_date in universe:
        partition_path = (
            f's3://{BUCKET_NAME}/ticker_prices/ticker={ticker}/data.parquet'
        )

        existing_df: Optional[pd.DataFrame] = None
        existing_dates: set = set()
        if s3.file_exists(partition_path):
            existing_df = s3.read_df_from_parquet(partition_path)
            if existing_df is not None and not existing_df.empty:
                existing_dates = set(existing_df['date'].astype(str).tolist())

        biz_days = {
            d.strftime('%Y-%m-%d')
            for d in pd.date_range(earliest_date, today, freq='B').date
        }
        missing = biz_days - existing_dates

        # Refresh tail: re-fetch recent business days every run so yfinance
        # back-corrections of recent prices land.
        tail_start = max(
            today - timedelta(days=TICKER_PRICES_REFRESH_TAIL_DAYS - 1),
            earliest_date,
        )
        missing |= {
            d.strftime('%Y-%m-%d')
            for d in pd.date_range(tail_start, today, freq='B').date
        }

        if not missing:
            continue

        fetch_start = date.fromisoformat(min(missing))
        fetch_end = date.fromisoformat(max(missing))

        try:
            fetched = fetch_yfinance_ticker_history(ticker, fetch_start, fetch_end)
        except Exception as e:
            logging.warning(f'  yfinance fetch failed for {ticker}: {e}')
            continue

        if fetched.empty:
            logging.info(
                f'  {ticker}: yfinance returned no rows for {fetch_start}..{fetch_end} '
                f'(no partition written; downstream reconstructs from transaction prices)'
            )
            continue

        if existing_df is not None and not existing_df.empty:
            combined = pd.concat([existing_df, fetched], ignore_index=True)
            combined = combined.drop_duplicates(subset=['date'], keep='last')
        else:
            combined = fetched

        combined = combined[cols].sort_values('date').reset_index(drop=True)
        s3.write_df_to_parquet(combined, partition_path)
        logging.info(f'  {ticker}: wrote {len(combined)} rows')


def etl_ynab_data() -> None:
    s3 = get_s3()
    budget_data = extract_budget_data()

    load_paystubs_from_sheets(s3)
    load_annual_contributions_from_sheets(s3)
    load_investment_transactions_from_sheets(s3)
    extract_category_groups(budget_data, s3)
    extract_categories(budget_data, s3)
    extract_transactions(budget_data, s3)
    extract_subtransactions(budget_data, s3)
    extract_accounts(budget_data, s3)
    extract_btc_wallet_history(s3)
    extract_btc_price_history(s3)
    extract_ticker_prices(s3)
