import io
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from src.etl.etl import extract_btc_price_history


@pytest.fixture(autouse=True)
def stable_bucket_name(monkeypatch):
    """Pin BUCKET_NAME so FakeS3.ls prefix parsing is predictable."""
    monkeypatch.setattr('src.etl.etl.BUCKET_NAME', 'test-bucket')


class FakeS3:
    """Match parquet files by suffix so tests don't depend on BUCKET_NAME."""

    def __init__(self, existing: dict | None = None) -> None:
        # keys are partition suffixes like
        # 'historical_btc_price/year=2024/month=01/day=15/data.parquet'
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
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow', index=False)
        self.writes.append((df.copy(), path))

    def ls(
        self, s3_uri: str, *, recursive: bool = True, include_prefixes: bool = False
    ):
        # Strip "s3://<bucket>/" so we can match against suffix-keyed entries.
        relative_prefix = s3_uri.split('/', 3)[-1] if s3_uri.count('/') >= 3 else s3_uri
        for suffix in self.existing:
            if suffix.startswith(relative_prefix):
                yield SimpleNamespace(uri=f's3://fake/{suffix}')


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload
        self.ok = True
        self.status_code = 200
        self.text = ''

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def partition_suffix(d: date) -> str:
    return (
        f'historical_btc_price/year={d.year}/month={d.month:02d}/'
        f'day={d.day:02d}/data.parquet'
    )


def partitions_for(prices_by_date: dict[str, float]) -> dict[str, pd.DataFrame]:
    """Build a {suffix: 1-row DataFrame} dict from a {iso_date: price} map."""
    out: dict[str, pd.DataFrame] = {}
    for iso, price in prices_by_date.items():
        d = date.fromisoformat(iso)
        out[partition_suffix(d)] = pd.DataFrame([{'date': iso, 'price_usd': price}])
    return out


def patch_coinbase_endpoint(monkeypatch, prices_by_iso_date: dict[str, float]):
    """Patch Coinbase /products/BTC-USD/candles.

    On each call, parses start/end from the ISO timestamps in params and emits
    a candle for every requested date that exists in `prices_by_iso_date`.
    Coinbase candle shape: [timestamp, low, high, open, close, volume].
    """
    captured: list = []

    def fake_get(url, *args, **kwargs):
        params = kwargs.get('params', {})
        captured.append({'url': url, 'params': dict(params)})
        start_dt = datetime.fromisoformat(params['start']).date()
        end_dt = datetime.fromisoformat(params['end']).date()  # exclusive
        candles = []
        for iso, price in prices_by_iso_date.items():
            d = date.fromisoformat(iso)
            if start_dt <= d < end_dt:
                ts = int(
                    datetime.combine(
                        d, datetime.min.time(), tzinfo=timezone.utc
                    ).timestamp()
                )
                candles.append([ts, price - 100, price + 100, price, price, 0.0])
        return FakeResponse(candles)

    monkeypatch.setattr('src.etl.etl.requests.get', fake_get)
    monkeypatch.setattr('src.etl.etl.time.sleep', lambda *_: None)
    return captured


def freeze_today(monkeypatch, today_iso: str):
    frozen = datetime.fromisoformat(today_iso).replace(tzinfo=timezone.utc)

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return frozen if tz is None else frozen.astimezone(tz)

    monkeypatch.setattr('src.etl.etl.datetime', FrozenDateTime)


def setup_window(monkeypatch, start_iso: str, tail_days: int):
    """Override start-date floor and refresh-tail length for a focused test."""
    monkeypatch.setattr(
        'src.etl.etl.BTC_PRICE_HISTORY_START_DATE', date.fromisoformat(start_iso)
    )
    monkeypatch.setattr('src.etl.etl.BTC_PRICE_REFRESH_TAIL_DAYS', tail_days)


def test_extract_btc_price_history_first_run_writes_each_date_in_window(monkeypatch):
    setup_window(monkeypatch, '2024-01-30', tail_days=0)
    freeze_today(monkeypatch, '2024-02-02')

    patch_coinbase_endpoint(
        monkeypatch,
        {
            '2024-01-30': 42_000.0,
            '2024-01-31': 42_500.0,
            '2024-02-01': 43_000.0,
            '2024-02-02': 43_500.0,
        },
    )
    s3 = FakeS3()

    extract_btc_price_history(s3)

    paths = sorted(w[1] for w in s3.writes)

    assert len(paths) == 4
    assert paths[0].endswith('year=2024/month=01/day=30/data.parquet')
    assert paths[3].endswith('year=2024/month=02/day=02/data.parquet')
    for df, _ in s3.writes:
        assert len(df) == 1
        assert set(df.columns) == {'date', 'price_usd'}


def test_extract_btc_price_history_incremental_only_writes_missing_dates(monkeypatch):
    setup_window(monkeypatch, '2024-01-30', tail_days=0)
    freeze_today(monkeypatch, '2024-02-02')

    # Coinbase returns candles for the whole requested range; we should only
    # WRITE the dates that weren't already present.
    patch_coinbase_endpoint(
        monkeypatch,
        {
            '2024-01-30': 42_000.0,
            '2024-01-31': 42_500.0,
            '2024-02-01': 43_000.0,
            '2024-02-02': 43_500.0,
        },
    )

    s3 = FakeS3(
        existing=partitions_for(
            {
                '2024-01-30': 42_000.0,
                '2024-01-31': 42_500.0,
            }
        )
    )

    extract_btc_price_history(s3)

    paths = sorted(w[1] for w in s3.writes)

    assert len(paths) == 2
    assert paths[0].endswith('year=2024/month=02/day=01/data.parquet')
    assert paths[1].endswith('year=2024/month=02/day=02/data.parquet')


def test_extract_btc_price_history_skips_when_window_already_complete(monkeypatch):
    setup_window(monkeypatch, '2024-01-30', tail_days=0)
    freeze_today(monkeypatch, '2024-01-31')

    monkeypatch.setattr(
        'src.etl.etl.requests.get',
        lambda *a, **kw: (_ for _ in ()).throw(
            AssertionError('should not be called when fully current')
        ),
    )
    monkeypatch.setattr('src.etl.etl.time.sleep', lambda *_: None)

    s3 = FakeS3(
        existing=partitions_for(
            {
                '2024-01-30': 42_000.0,
                '2024-01-31': 42_500.0,
            }
        )
    )

    extract_btc_price_history(s3)

    assert s3.writes == []


def test_extract_btc_price_history_refresh_tail_overwrites_recent_days(monkeypatch):
    """Even when last 3 days exist, they're refetched and overwritten."""
    setup_window(monkeypatch, '2024-01-25', tail_days=3)
    freeze_today(monkeypatch, '2024-01-31')

    # Coinbase returns the corrected prices for the last 3 days
    patch_coinbase_endpoint(
        monkeypatch,
        {
            '2024-01-29': 99_000.0,  # corrected
            '2024-01-30': 99_500.0,  # corrected
            '2024-01-31': 100_000.0,  # new
        },
    )

    s3 = FakeS3(
        existing=partitions_for(
            {
                '2024-01-25': 41_000.0,
                '2024-01-26': 41_500.0,
                '2024-01-27': 42_000.0,
                '2024-01-28': 42_500.0,
                '2024-01-29': 50_000.0,  # stale
                '2024-01-30': 50_500.0,  # stale
            }
        )
    )

    extract_btc_price_history(s3)

    paths_to_prices = {p: df.iloc[0]['price_usd'] for df, p in s3.writes}
    suffix_to_price = {
        next(
            suf
            for suf in [
                'year=2024/month=01/day=29/data.parquet',
                'year=2024/month=01/day=30/data.parquet',
                'year=2024/month=01/day=31/data.parquet',
            ]
            if p.endswith(suf)
        ): v
        for p, v in paths_to_prices.items()
    }

    assert suffix_to_price['year=2024/month=01/day=29/data.parquet'] == 99_000.0
    assert suffix_to_price['year=2024/month=01/day=30/data.parquet'] == 99_500.0
    assert suffix_to_price['year=2024/month=01/day=31/data.parquet'] == 100_000.0
    # 2024-01-25..28 should NOT be re-written
    for _, p in s3.writes:
        for early_suf in [
            'day=25/data.parquet',
            'day=26/data.parquet',
            'day=27/data.parquet',
            'day=28/data.parquet',
        ]:
            assert not p.endswith(early_suf)


def test_extract_btc_price_history_respects_start_date_floor(monkeypatch):
    setup_window(monkeypatch, '2024-02-01', tail_days=0)
    freeze_today(monkeypatch, '2024-02-02')

    captured = patch_coinbase_endpoint(
        monkeypatch,
        {
            '2024-02-01': 50_000.0,
            '2024-02-02': 50_500.0,
        },
    )
    s3 = FakeS3()

    extract_btc_price_history(s3)

    # All requested ranges should start at or after the floor.
    for c in captured:
        start_dt = datetime.fromisoformat(c['params']['start']).date()
        assert start_dt >= date(2024, 2, 1)


def test_extract_btc_price_history_skips_when_no_candles_returned(monkeypatch):
    setup_window(monkeypatch, '2024-03-04', tail_days=0)
    freeze_today(monkeypatch, '2024-03-05')

    # empty candles array — Coinbase has no data for these dates
    monkeypatch.setattr('src.etl.etl.requests.get', lambda *a, **kw: FakeResponse([]))
    monkeypatch.setattr('src.etl.etl.time.sleep', lambda *_: None)
    s3 = FakeS3()

    extract_btc_price_history(s3)

    assert s3.writes == []


def test_extract_btc_price_history_chunks_long_backfill(monkeypatch):
    """A 350-day backfill should result in multiple Coinbase calls."""
    setup_window(monkeypatch, '2023-01-01', tail_days=0)
    freeze_today(monkeypatch, '2023-12-16')  # ~350 days

    # Generate a synthetic price for every day in the range
    full_range = pd.date_range(start='2023-01-01', end='2023-12-16', freq='D').date
    prices = {d.isoformat(): 30_000.0 + i for i, d in enumerate(full_range)}

    captured = patch_coinbase_endpoint(monkeypatch, prices)
    s3 = FakeS3()

    extract_btc_price_history(s3)

    # 350 days / 300 per chunk = 2 calls
    assert len(captured) == 2
    # All days written
    assert len(s3.writes) == len(full_range)
