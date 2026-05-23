import io
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from src.etl.etl import (
    extract_ticker_prices,
    fetch_yfinance_ticker_history,
    get_ticker_price_universe,
)


@pytest.fixture(autouse=True)
def stable_bucket_name(monkeypatch):
    monkeypatch.setattr('src.etl.etl.BUCKET_NAME', 'test-bucket')


class FakeS3:
    """Match parquet files by suffix so tests don't depend on BUCKET_NAME."""

    def __init__(self, existing: dict | None = None) -> None:
        self.existing: dict[str, pd.DataFrame] = existing or {}
        self.writes: list[tuple[pd.DataFrame, str]] = []

    def match(self, path: str) -> str | None:
        for suffix in self.existing:
            if path.endswith(suffix):
                return suffix
        return None

    def file_exists(self, path: str) -> bool:
        return self.match(path) is not None

    def read_df_from_parquet(self, path: str) -> pd.DataFrame:
        suffix = self.match(path)
        return self.existing[suffix].copy()

    def write_df_to_parquet(self, df: pd.DataFrame, path: str) -> None:
        # Round-trip through pyarrow to mirror the real failure mode.
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow', index=False)
        self.writes.append((df.copy(), path))


def make_investment_transactions(
    symbols_and_dates: list[tuple[str, str]],
) -> pd.DataFrame:
    """Build a minimal investment-transactions parquet stub with required columns."""
    rows = []
    for sym, dt in symbols_and_dates:
        rows.append(
            {
                'source_file_name': 'fake.pdf',
                'broker': 'test',
                'account_number': 'X',
                'trade_date': dt,
                'settlement_date': '',
                'type': 'BUY',
                'symbol': sym,
                'description': '',
                'quantity': '1',
                'price': '',
                'amount': '',
                'fees': '',
                'currency': 'USD',
                'raw_json': '{}',
            }
        )
    return pd.DataFrame(rows)


class FakeTickerHistory:
    """Patch target for ``yfinance.Ticker(ticker).history(...)``.

    Each call records args and returns a yfinance-shaped DataFrame indexed by Date.
    Customize per-ticker behavior by passing a dict {ticker: {iso_date: price}}.
    """

    def __init__(self, prices_by_ticker: dict[str, dict[str, float]]):
        self.prices = prices_by_ticker
        self.calls: list[dict] = []

    def __call__(self, ticker):
        outer = self

        class _Ticker:
            def history(self, *, start, end, auto_adjust):
                outer.calls.append(
                    {
                        'ticker': ticker,
                        'start': start,
                        'end': end,
                        'auto_adjust': auto_adjust,
                    }
                )
                rows = []
                idx_dates = []
                for iso, price in outer.prices.get(ticker, {}).items():
                    d = date.fromisoformat(iso)
                    if start <= d < end:
                        idx_dates.append(d)
                        rows.append(
                            {
                                'Open': price - 1.0,
                                'High': price + 0.5,
                                'Low': price - 1.5,
                                'Close': price,
                                'Adj Close': price * 0.99,
                                'Volume': 1_000_000,
                            }
                        )
                df = pd.DataFrame(rows)
                if df.empty:
                    return df
                df.index = pd.to_datetime(idx_dates).tz_localize('UTC')
                df.index.name = 'Date'
                return df

        return _Ticker()


def freeze_today(monkeypatch, today_iso: str):
    frozen = datetime.fromisoformat(today_iso).replace(tzinfo=timezone.utc)

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return frozen if tz is None else frozen.astimezone(tz)

    monkeypatch.setattr('src.etl.etl.datetime', FrozenDateTime)


def patch_yf(monkeypatch, prices_by_ticker):
    fake = FakeTickerHistory(prices_by_ticker)
    monkeypatch.setattr('src.etl.etl.yf.Ticker', fake)
    return fake


def stub_universe(s3: FakeS3, symbols_and_dates):
    """Pre-populate the FakeS3 with the investment transactions parquet."""
    s3.existing['raw-investment-transactions.parquet'] = make_investment_transactions(
        symbols_and_dates
    )


def test_get_ticker_price_universe_filters_empty_and_multiword():
    """Empty symbols (cash events) and multi-word fund names get filtered.
    Money-market funds like FDRXX flow through — they get a $1 NAV default via
    downstream coalesce when yfinance returns nothing."""
    s3 = FakeS3()
    stub_universe(
        s3,
        [
            ('VTI', '2024-01-15'),
            ('VTI', '2024-03-20'),
            ('LAW', '2021-06-29'),
            ('FDRXX', '2024-01-01'),  # money market — kept
            ('TRP RETIRE 2065 F', '2024-08-22'),  # multi-word, no real ticker
            ('', '2024-06-30'),  # empty (cash event)
        ],
    )

    universe = get_ticker_price_universe(s3)

    assert universe == [
        ('FDRXX', date(2024, 1, 1)),
        ('LAW', date(2021, 6, 29)),
        ('VTI', date(2024, 1, 15)),
    ]


def test_get_ticker_price_universe_returns_empty_when_parquet_missing():
    s3 = FakeS3()
    assert get_ticker_price_universe(s3) == []


def test_extract_ticker_prices_first_run_writes_one_file_per_ticker(monkeypatch):
    freeze_today(monkeypatch, '2024-02-05')
    s3 = FakeS3()
    stub_universe(s3, [('VTI', '2024-01-30'), ('AAPL', '2024-01-30')])
    patch_yf(
        monkeypatch,
        {
            'VTI': {
                '2024-01-30': 240.00,
                '2024-01-31': 240.50,
                '2024-02-01': 241.00,
                '2024-02-02': 241.50,
                '2024-02-05': 242.00,  # 2/3-2/4 weekend
            },
            'AAPL': {
                '2024-01-30': 188.00,
                '2024-01-31': 184.00,
                '2024-02-01': 186.00,
                '2024-02-02': 185.00,
                '2024-02-05': 187.00,
            },
        },
    )

    extract_ticker_prices(s3)

    paths = sorted(w[1] for w in s3.writes)
    assert len(paths) == 2
    assert paths[0].endswith('ticker_prices/ticker=AAPL/data.parquet')
    assert paths[1].endswith('ticker_prices/ticker=VTI/data.parquet')

    by_path = {p: df for df, p in s3.writes}
    vti_df = next(df for p, df in by_path.items() if 'ticker=VTI' in p)
    assert vti_df['date'].tolist() == [
        '2024-01-30',
        '2024-01-31',
        '2024-02-01',
        '2024-02-02',
        '2024-02-05',
    ]
    assert vti_df['close'].tolist() == [240.00, 240.50, 241.00, 241.50, 242.00]
    assert set(vti_df.columns) == {
        'date',
        'open',
        'high',
        'low',
        'close',
        'adj_close',
        'volume',
    }


def test_extract_ticker_prices_one_yfinance_call_per_ticker(monkeypatch):
    """All-time backfill should be a single yfinance.history() call per ticker."""
    freeze_today(monkeypatch, '2024-06-30')
    s3 = FakeS3()
    stub_universe(s3, [('VTI', '2022-01-03')])
    fake = patch_yf(monkeypatch, {'VTI': {'2024-06-28': 250.0}})

    extract_ticker_prices(s3)

    vti_calls = [c for c in fake.calls if c['ticker'] == 'VTI']
    assert len(vti_calls) == 1
    # Single call covers the full needed range from earliest_date through the
    # latest business day on/before today (2024-06-30 is a Sunday → Fri 6/28).
    assert vti_calls[0]['start'] == date(2022, 1, 3)
    assert vti_calls[0]['end'] == date(2024, 6, 29)  # max(missing)=6/28, bumped +1


def test_extract_ticker_prices_skips_when_fully_current(monkeypatch):
    """If the file has every business day from earliest to today (modulo the
    refresh tail), the tail still triggers a fetch but no new rows are written
    when yfinance returns the same data already on disk."""
    freeze_today(monkeypatch, '2024-02-05')  # Mon
    s3 = FakeS3()
    stub_universe(s3, [('VTI', '2024-01-29')])

    all_biz = [
        d.strftime('%Y-%m-%d')
        for d in pd.date_range('2024-01-29', '2024-02-05', freq='B').date
    ]
    s3.existing['ticker_prices/ticker=VTI/data.parquet'] = pd.DataFrame(
        {
            'date': all_biz,
            'open': [240.0] * len(all_biz),
            'high': [241.0] * len(all_biz),
            'low': [239.0] * len(all_biz),
            'close': [240.5] * len(all_biz),
            'adj_close': [240.0] * len(all_biz),
            'volume': [1_000_000] * len(all_biz),
        }
    )

    # yfinance returns NOTHING — tail fetch happens but no new data → no write
    fake = patch_yf(monkeypatch, {'VTI': {}})

    extract_ticker_prices(s3)

    assert s3.writes == []
    # Confirm the refresh tail did trigger a call (defensive)
    assert any(c['ticker'] == 'VTI' for c in fake.calls)


def test_extract_ticker_prices_backfills_missing_dates(monkeypatch):
    """Partial existing file + new dates → fetch and merge."""
    freeze_today(monkeypatch, '2024-02-05')
    s3 = FakeS3()
    stub_universe(s3, [('VTI', '2024-01-29')])

    # File has only 2 of the 6 business days through 2024-02-05
    s3.existing['ticker_prices/ticker=VTI/data.parquet'] = pd.DataFrame(
        {
            'date': ['2024-01-29', '2024-01-30'],
            'open': [240.0, 241.0],
            'high': [241.0, 242.0],
            'low': [239.0, 240.0],
            'close': [240.5, 241.5],
            'adj_close': [240.0, 241.0],
            'volume': [1_000_000, 1_000_000],
        }
    )
    patch_yf(
        monkeypatch,
        {
            'VTI': {
                '2024-01-31': 242.0,
                '2024-02-01': 243.0,
                '2024-02-02': 244.0,
                '2024-02-05': 245.0,
            }
        },
    )

    extract_ticker_prices(s3)

    assert len(s3.writes) == 1
    df, _ = s3.writes[0]
    assert df['date'].tolist() == [
        '2024-01-29',
        '2024-01-30',
        '2024-01-31',
        '2024-02-01',
        '2024-02-02',
        '2024-02-05',
    ]


def test_extract_ticker_prices_refresh_tail_overwrites_recent_days(monkeypatch):
    """Recent-tail business days are re-fetched on every run; yfinance corrections win."""
    freeze_today(monkeypatch, '2024-02-15')  # Thu
    s3 = FakeS3()
    stub_universe(s3, [('VTI', '2024-02-01')])

    feb_biz = [
        d.strftime('%Y-%m-%d')
        for d in pd.date_range('2024-02-01', '2024-02-15', freq='B').date
    ]
    s3.existing['ticker_prices/ticker=VTI/data.parquet'] = pd.DataFrame(
        {
            'date': feb_biz,
            'open': [240.0] * len(feb_biz),
            'high': [241.0] * len(feb_biz),
            'low': [239.0] * len(feb_biz),
            'close': [99.99] * len(feb_biz),  # stale — should be overwritten on tail
            'adj_close': [99.0] * len(feb_biz),
            'volume': [1_000_000] * len(feb_biz),
        }
    )

    # yfinance returns CORRECTED prices for the recent tail
    patch_yf(
        monkeypatch,
        {
            'VTI': {
                '2024-02-09': 250.0,
                '2024-02-12': 251.0,
                '2024-02-13': 252.0,
                '2024-02-14': 253.0,
                '2024-02-15': 254.0,
            }
        },
    )

    extract_ticker_prices(s3)

    assert len(s3.writes) == 1
    df, _ = s3.writes[0]
    closes = dict(zip(df['date'], df['close']))
    assert closes['2024-02-15'] == 254.0  # corrected, not 99.99
    assert closes['2024-02-14'] == 253.0
    assert closes['2024-02-09'] == 250.0
    # Older days outside the tail keep their (stale) values — only the tail
    # is force-refetched, so e.g. 2024-02-01 is still 99.99
    assert closes['2024-02-01'] == 99.99


def test_extract_ticker_prices_skips_when_yfinance_returns_nothing(monkeypatch):
    """Tickers yfinance has no data for (money-market funds, exotic, typos) write
    no rows — no partition is created. Downstream reconstruction sources prices
    from cleaned.investment_transactions for absent (ticker, date) pairs."""
    freeze_today(monkeypatch, '2024-02-05')
    s3 = FakeS3()
    stub_universe(s3, [('FDRXX', '2024-02-01')])
    patch_yf(monkeypatch, {})  # yfinance has no data for FDRXX

    extract_ticker_prices(s3)

    assert s3.writes == []


def test_extract_ticker_prices_handles_yfinance_exception(monkeypatch):
    freeze_today(monkeypatch, '2024-02-05')
    s3 = FakeS3()
    stub_universe(s3, [('VTI', '2024-02-01')])

    def raising_ticker(ticker):
        class _T:
            def history(self, **kwargs):
                raise RuntimeError('yahoo down')

        return _T()

    monkeypatch.setattr('src.etl.etl.yf.Ticker', raising_ticker)

    extract_ticker_prices(s3)  # should not raise

    assert s3.writes == []


def test_fetch_yfinance_ticker_history_passes_correct_range(monkeypatch):
    """end-date should be bumped by one day so the requested end is inclusive."""
    fake = FakeTickerHistory({'VTI': {'2024-01-31': 241.0, '2024-02-01': 242.0}})
    monkeypatch.setattr('src.etl.etl.yf.Ticker', fake)

    df = fetch_yfinance_ticker_history('VTI', date(2024, 1, 31), date(2024, 2, 1))

    assert fake.calls[0]['ticker'] == 'VTI'
    assert fake.calls[0]['start'] == date(2024, 1, 31)
    assert fake.calls[0]['end'] == date(2024, 2, 2)  # bumped +1
    assert fake.calls[0]['auto_adjust'] is False
    assert df['date'].tolist() == ['2024-01-31', '2024-02-01']
    assert df['close'].tolist() == [241.0, 242.0]
