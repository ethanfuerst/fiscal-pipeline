MODEL (
  name cleaned.investment_transactions,
  kind FULL,
  grain (source_file_name, broker, account_number, trade_date, symbol, type, amount),
  description 'Cleaned investment transactions from broker statements. Dates parsed from ISO strings to DATE. Money values cast from string to DECIMAL; empty source values become NULL (not zero — for SPLIT rows amount is genuinely empty). Sign convention on amount: positive = cash INTO the account, negative = cash OUT. See ~/Documents/Google Drive/Money/Investment Account Statements/EXTRACTION_NOTES.md for the canonical type vocabulary and per-broker classification rules.'
);

select
    /* Provenance */
    source_file_name  -- Source statement basename (joins to the manifest in the statements folder)
    , broker  -- Broker key, lowercase: schwab, fidelity, etrade, …
    , account_number  -- Account number, preserved verbatim from the source statement

    /* Timestamps */
    , try_cast(nullif(trade_date, '') as date) as trade_date  -- Trade date parsed from ISO string
    , try_cast(nullif(settlement_date, '') as date) as settlement_date  -- Settlement date parsed from ISO string (may be null)

    /* Classification */
    , type  -- Canonical type: BUY, SELL, DIVIDEND, INTEREST, DEPOSIT, WITHDRAWAL, TRANSFER_IN, TRANSFER_OUT, FEE, SPLIT, OTHER
    , symbol  -- Trading symbol, empty for cash-only events
    , description  -- Broker free-text description, preserved verbatim for audit
    , currency  -- ISO currency code, default USD

    /* Numerics — empty string → NULL (preserves "unknown" vs. zero) */
    , try_cast(nullif(quantity, '') as decimal(38, 8)) as quantity  -- Share quantity (8-decimal precision for fractional shares)
    , try_cast(nullif(price, '') as decimal(20, 8)) as price  -- Per-share price
    , try_cast(nullif(amount, '') as decimal(20, 4)) as amount  -- Signed cash impact in USD (positive = into account, negative = out)
    , try_cast(nullif(fees, '') as decimal(20, 4)) as fees  -- Broker fees / commissions, when broken out separately

    /* Audit escape hatch */
    , raw_json  -- JSON of any source fields not promoted to columns (broker quirks, ACAT annotations, etc.)
from raw.investment_transactions
order by trade_date desc, broker, account_number, symbol
