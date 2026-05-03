MODEL (
  name cleaned.btc_wallet_history,
  kind FULL,
  grain (wallet_address, txid),
  description 'Cleaned BTC wallet transactions, one row per (wallet_address, txid) with per-wallet sent/received attribution computed from the raw vin/vout JSON. Confirmed transactions only.'
);

with entries as (
    -- Unnest both vout (received) and vin (sent) entries for every tx into a
    -- single flat list tagged by direction, so we can attribute in one CTE.
    select
        h.wallet_address
        , h.txid
        , 'received' as direction
        , json_extract_string(x, '$.scriptpubkey_address') as addr
        , cast(json_extract(x, '$.value') as bigint) as value
    from raw.btc_wallet_history h
        , unnest(cast(json_extract(h.vout_json, '$[*]') as json[])) as x(x)
    union all
    select
        h.wallet_address
        , h.txid
        , 'sent' as direction
        , json_extract_string(x, '$.prevout.scriptpubkey_address') as addr
        , cast(json_extract(x, '$.prevout.value') as bigint) as value
    from raw.btc_wallet_history h
        , unnest(cast(json_extract(h.vin_json, '$[*]') as json[])) as x(x)
)

, attribution as (
    select
        wallet_address
        , txid
        , coalesce(sum(value) filter (where direction = 'received'), 0) as received_sats
        , coalesce(sum(value) filter (where direction = 'sent'), 0) as sent_sats
    from entries
    where addr = wallet_address
    group by wallet_address, txid
)

select
    /* Primary key */
    h.wallet_address  -- BTC wallet address this row attributes to
    , h.txid  -- Bitcoin transaction id

    /* Timestamps */
    , h.block_time  -- Unix epoch seconds when block was mined
    , to_timestamp(h.block_time) as transaction_timestamp  -- UTC timestamp parsed from block_time
    , cast(to_timestamp(h.block_time) as date) as transaction_date  -- UTC date parsed from block_time

    /* Status and properties */
    , h.block_height  -- Block height where this transaction was confirmed
    , h.confirmed  -- Whether the transaction is confirmed on-chain
    , h.size  -- Tx size in bytes
    , h.weight  -- Tx weight (BIP141)
    , h.vin_json  -- Raw vin array from Esplora as JSON string (input list)
    , h.vout_json  -- Raw vout array from Esplora as JSON string (output list)

    /* Money (sats) */
    , coalesce(x.sent_sats, 0) as sent_sats  -- Sats sent FROM this wallet (sum of matching tx inputs)
    , coalesce(x.received_sats, 0) as received_sats  -- Sats received BY this wallet (sum of matching tx outputs)
    , coalesce(x.received_sats, 0) - coalesce(x.sent_sats, 0) as net_sats  -- Net sats delta to this wallet (positive = received)
    , h.fee as fee_sats  -- Total tx fee in sats (not attributed to any single party)

    /* Money (BTC) */
    , coalesce(x.sent_sats, 0) / 100000000.0 as sent_btc  -- BTC sent FROM this wallet
    , coalesce(x.received_sats, 0) / 100000000.0 as received_btc  -- BTC received BY this wallet
    , (coalesce(x.received_sats, 0) - coalesce(x.sent_sats, 0)) / 100000000.0 as net_btc  -- Net BTC delta to this wallet
    , h.fee / 100000000.0 as fee_btc  -- Total tx fee in BTC
from raw.btc_wallet_history h
left join attribution as x using (wallet_address, txid)
where h.confirmed
order by
    transaction_timestamp desc
    , wallet_address
