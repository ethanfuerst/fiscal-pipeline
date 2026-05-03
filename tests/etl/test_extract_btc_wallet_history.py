import io
import json

import pandas as pd

from src.etl.etl import extract_btc_wallet_history


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
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow', index=False)
        self.writes.append((df.copy(), path))


def confirmed_tx(
    txid: str,
    *,
    wallet: str,
    block_height: int,
    block_time: int = 1_700_000_000,
    received_sats: int = 0,
    sent_sats: int = 0,
    fee: int = 250,
) -> dict:
    return {
        'txid': txid,
        'fee': fee,
        'size': 200,
        'weight': 800,
        'vin': (
            [
                {
                    'prevout': {
                        'scriptpubkey_address': wallet,
                        'value': sent_sats,
                    }
                }
            ]
            if sent_sats
            else []
        ),
        'vout': (
            [{'scriptpubkey_address': wallet, 'value': received_sats}]
            if received_sats
            else []
        ),
        'status': {
            'confirmed': True,
            'block_height': block_height,
            'block_time': block_time,
        },
    }


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def patch_get_with_map(monkeypatch, url_to_payload: dict[str, list]):
    seen: list[str] = []

    def fake_get(url, *args, **kwargs):
        seen.append(url)
        for key, payload in url_to_payload.items():
            if url.endswith(key):
                return FakeResponse(payload)
        raise AssertionError(f'unexpected GET {url}')

    monkeypatch.setattr('src.etl.etl.requests.get', fake_get)
    monkeypatch.setattr('src.etl.etl.time.sleep', lambda *_: None)
    return seen


def test_extract_btc_wallet_history_writes_all_txs_for_each_wallet(monkeypatch):
    wallet_a = 'bc1qaaaa'
    wallet_b = 'bc1qbbbb'
    monkeypatch.setenv('BTC_ADDRESS_LIST', f'{wallet_a},{wallet_b}')

    patch_get_with_map(
        monkeypatch,
        {
            f'/address/{wallet_a}/txs': [
                confirmed_tx(
                    'tx_a1', wallet=wallet_a, block_height=200, received_sats=10_000
                )
            ],
            f'/address/{wallet_a}/txs/chain/tx_a1': [],
            f'/address/{wallet_b}/txs': [
                confirmed_tx(
                    'tx_b1', wallet=wallet_b, block_height=210, sent_sats=5_000
                )
            ],
            f'/address/{wallet_b}/txs/chain/tx_b1': [],
        },
    )
    s3 = FakeS3()

    extract_btc_wallet_history(s3)

    assert len(s3.writes) == 1
    df, path = s3.writes[0]
    assert path.endswith('/btc-wallet-history.parquet')
    assert sorted(df['txid'].tolist()) == ['tx_a1', 'tx_b1']
    row_a = df[df['txid'] == 'tx_a1'].iloc[0]
    assert row_a['wallet_address'] == wallet_a
    assert row_a['received_sats'] == 10_000
    assert row_a['sent_sats'] == 0
    assert bool(row_a['confirmed'])
    assert json.loads(row_a['vout_json'])[0]['value'] == 10_000


def test_extract_btc_wallet_history_paginates_through_multiple_pages(monkeypatch):
    """Walks /txs then /txs/chain/{last_seen_txid} until empty, full overwrite."""
    wallet = 'bc1qaaaa'
    monkeypatch.setenv('BTC_ADDRESS_LIST', wallet)

    seen = patch_get_with_map(
        monkeypatch,
        {
            f'/address/{wallet}/txs': [
                confirmed_tx('tx_p1_a', wallet=wallet, block_height=300),
                confirmed_tx('tx_p1_b', wallet=wallet, block_height=290),
            ],
            f'/address/{wallet}/txs/chain/tx_p1_b': [
                confirmed_tx('tx_p2_a', wallet=wallet, block_height=200),
            ],
            f'/address/{wallet}/txs/chain/tx_p2_a': [],
        },
    )
    s3 = FakeS3()

    extract_btc_wallet_history(s3)

    df, _ = s3.writes[0]

    assert sorted(df['txid'].tolist()) == ['tx_p1_a', 'tx_p1_b', 'tx_p2_a']
    # full pagination walk: 3 GETs (initial + 2 chain calls, last returns [])
    assert len(seen) == 3


def test_extract_btc_wallet_history_overwrites_existing_parquet(monkeypatch):
    """Each run is a full overwrite — old rows are NOT preserved."""
    wallet = 'bc1qaaaa'
    monkeypatch.setenv('BTC_ADDRESS_LIST', wallet)

    patch_get_with_map(
        monkeypatch,
        {
            f'/address/{wallet}/txs': [
                confirmed_tx(
                    'tx_new', wallet=wallet, block_height=500, received_sats=9_999
                )
            ],
            f'/address/{wallet}/txs/chain/tx_new': [],
        },
    )

    # an existing parquet with a stale tx the API no longer returns
    existing_df = pd.DataFrame(
        [
            {
                'wallet_address': wallet,
                'txid': 'tx_stale',
                'block_height': 100,
                'block_time': 1_500_000_000,
                'confirmed': True,
                'sent_sats': 0,
                'received_sats': 1,
                'fee_sats': 0,
                'size': 1,
                'weight': 1,
                'vin_json': '[]',
                'vout_json': '[]',
            }
        ]
    )
    s3 = FakeS3(existing={'btc-wallet-history.parquet': existing_df})

    extract_btc_wallet_history(s3)

    df, _ = s3.writes[0]

    assert df['txid'].tolist() == ['tx_new']
    assert df.iloc[0]['received_sats'] == 9_999


def test_extract_btc_wallet_history_empty_address_list_skips(monkeypatch):
    monkeypatch.setenv('BTC_ADDRESS_LIST', '')

    monkeypatch.setattr(
        'src.etl.etl.requests.get',
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError('should not be called')),
    )
    s3 = FakeS3()

    extract_btc_wallet_history(s3)

    assert s3.writes == []
