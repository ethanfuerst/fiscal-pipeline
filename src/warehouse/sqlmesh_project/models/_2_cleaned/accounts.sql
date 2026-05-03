MODEL (
  name cleaned.accounts,
  kind FULL,
  grain id,
  description 'Cleaned YNAB accounts.'
);

select
    /* Primary key */
    id  -- YNAB account UUID

    /* Foreign keys */
    , transfer_payee_id  -- Payee UUID used for transfers into and out of this account

    /* Timestamps */
    , last_reconciled_at  -- Timestamp of the last reconciliation

    /* Status and properties */
    , name  -- Account display name
    , type  -- Account type (checking, savings, creditCard, cash, lineOfCredit, otherAsset, otherLiability, mortgage, autoLoan, studentLoan, personalLoan, medicalDebt, otherDebt)
    , on_budget  -- Whether the account contributes to the budget
    , closed  -- Whether the account has been closed
    , note  -- User-entered note (nullable)
    , direct_import_linked  -- Whether direct bank import is linked
    , direct_import_in_error  -- Whether the direct import is currently in error
    , debt_original_balance  -- Original debt balance in milliunits (debt-type accounts only, nullable)
    , deleted  -- Whether the account has been deleted
    , coalesce(on_budget, false) as is_on_budget  -- True when on_budget = true (NULL coerced to false)
    , coalesce(closed, false) as is_closed  -- True when closed = true (NULL coerced to false)

    /* Money */
    , balance  -- Working balance in milliunits
    , balance / 10 as balance_cents  -- Working balance in cents
    , balance / 1000 as balance_usd  -- Working balance in USD
    , cleared_balance  -- Cleared balance in milliunits
    , cleared_balance / 10 as cleared_balance_cents  -- Cleared balance in cents
    , cleared_balance / 1000 as cleared_balance_usd  -- Cleared balance in USD
    , uncleared_balance  -- Uncleared balance in milliunits
    , uncleared_balance / 10 as uncleared_balance_cents  -- Uncleared balance in cents
    , uncleared_balance / 1000 as uncleared_balance_usd  -- Uncleared balance in USD
from raw.accounts
