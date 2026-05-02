MODEL (
  name cleaned.transactions,
  kind FULL,
  grain id,
  description 'Cleaned YNAB transactions. Excludes rows without a category_id.'
);

select
    /* Primary key */
    id  -- YNAB transaction UUID

    /* Foreign keys */
    , account_id  -- Account UUID
    , payee_id  -- Payee UUID (nullable)
    , category_id  -- Category UUID (filtered not null below)
    , transfer_account_id  -- Account UUID this transaction transfers to (nullable)
    , transfer_transaction_id  -- Paired transfer transaction UUID (nullable)
    , matched_transaction_id  -- Matched bank-import transaction UUID (nullable)
    , import_id  -- Import-side identifier used for dedupe (nullable)

    /* Timestamps */
    , date  -- Transaction date as a raw ISO date string

    /* Status and properties */
    , memo  -- Free-form memo (nullable)
    , cleared  -- Cleared status (cleared, uncleared, reconciled)
    , approved  -- Whether the user has approved the transaction
    , flag_color  -- red, orange, yellow, green, blue, purple, nullable
    , import_payee_name  -- Cleaned payee name from bank import (nullable)
    , import_payee_name_original  -- Original payee name from bank import before any cleanup (nullable)
    , debt_transaction_type  -- Debt-related transaction type (nullable)
    , deleted  -- Whether the transaction has been deleted

    /* Money */
    , amount  -- Transaction amount, milliunits (negative = outflow)
    , amount / 10 as amount_cents  -- Amount in cents
    , amount / 1000 as amount_usd  -- Amount in USD

    /* Derived columns */
    , strptime(date, '%Y-%m-%d') as transaction_date  -- Parsed transaction date
    , import_payee_name as payee_name  -- Convenience copy of import_payee_name
    , case
        when memo like '%filename:%'
            then trim(regexp_extract(memo, 'filename:\s*(.+?)\s*$', 1))
    end as paystub_file_name  -- Paystub file_name parsed from memos formatted "<text> filename:<paystub_file_name>" at end of memo (nullable)
from raw.transactions
where category_id is not null
order by
    transaction_date desc
    , amount_usd desc
