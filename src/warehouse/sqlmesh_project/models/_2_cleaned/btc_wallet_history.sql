MODEL (
  name cleaned.btc_wallet_history,
  kind FULL,
  grain (wallet_address, txid),
  description 'Cleaned BTC wallet transactions, one row per (wallet_address, txid) with net delta to that wallet. Confirmed transactions only.'
);

select
    /* Primary key */
    wallet_address  -- BTC wallet address this row attributes to
    , txid  -- Bitcoin transaction id

    /* Timestamps */
    , block_time  -- Unix epoch seconds when block was mined (nullable for unconfirmed; filtered below)
    , to_timestamp(block_time) as transaction_timestamp  -- UTC timestamp parsed from block_time
    , cast(to_timestamp(block_time) as date) as transaction_date  -- UTC date parsed from block_time

    /* Status and properties */
    , block_height  -- Block height where this transaction was confirmed
    , confirmed  -- Whether the transaction is confirmed on-chain
    , size  -- Tx size in bytes
    , weight  -- Tx weight (BIP141)
    , vin_json  -- Raw vin array from Esplora as JSON string (input list)
    , vout_json  -- Raw vout array from Esplora as JSON string (output list)

    /* Money (sats) */
    , sent_sats  -- Sats sent FROM this wallet (sum of matching tx inputs)
    , received_sats  -- Sats received BY this wallet (sum of matching tx outputs)
    , received_sats - sent_sats as net_sats  -- Net sats delta to this wallet (positive = received)
    , fee_sats  -- Total tx fee in sats (not attributed to any single party)

    /* Money (BTC) */
    , sent_sats / 100000000.0 as sent_btc  -- BTC sent FROM this wallet
    , received_sats / 100000000.0 as received_btc  -- BTC received BY this wallet
    , (received_sats - sent_sats) / 100000000.0 as net_btc  -- Net BTC delta to this wallet
    , fee_sats / 100000000.0 as fee_btc  -- Total tx fee in BTC
from raw.btc_wallet_history
where confirmed
order by
    transaction_timestamp desc
    , wallet_address
