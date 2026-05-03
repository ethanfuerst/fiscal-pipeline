MODEL (
  name cleaned.sub_transactions,
  kind FULL,
  grain id,
  description 'Cleaned YNAB sub-transactions.'
);

select
    /* Primary key */
    id  -- YNAB sub-transaction UUID

    /* Foreign keys */
    , transaction_id  -- Parent transaction UUID
    , payee_id  -- Payee UUID (nullable)
    , category_id  -- Category UUID (nullable)
    , transfer_account_id  -- Account UUID this split transfers to (nullable)

    /* Status and properties */
    , memo  -- Free-form memo (nullable)
    , deleted  -- Whether the sub-transaction has been deleted

    /* Money */
    , amount  -- Sub-transaction amount, milliunits (negative = outflow)
    , amount / 10 as amount_cents  -- Amount in cents
    , amount / 1000 as amount_usd  -- Amount in USD
from raw.subtransactions
