MODEL (
  name combined.transactions,
  kind FULL,
  grain id
);

with all_transactions as (
    select
        transactions.id
        , transactions.account_id
        , transactions.category_id
        , transactions.amount_usd as amount
        , transactions.memo
        , transactions.paystub_file_name
        , subtransactions.id as subtransaction_id
        , subtransactions.category_id as sub_category_id
        , subtransactions.amount_usd as subtransaction_amount
        , subtransactions.memo as subtransaction_memo
        , transactions.transaction_date
    from cleaned.transactions as transactions
    left outer join cleaned.sub_transactions as subtransactions
        on transactions.id = subtransactions.transaction_id
)

, transactions_int as (
    select
        id
        , account_id
        , coalesce(sub_category_id, category_id) as category_id
        , coalesce(subtransaction_amount, amount) as amount
        , coalesce(subtransaction_memo, memo) as memo
        , paystub_file_name
        , transaction_date
    from all_transactions
)

select
    transactions_int.id
    , transactions_int.transaction_date
    , categories.id as category_id
    , categories.category_group_id
    , categories.category_name
    , category_groups.category_group_name_mapping
    , category_groups.subcategory_group_name
    , transactions_int.amount
    , transactions_int.memo
    , transactions_int.paystub_file_name
    , accounts.name as account_name
    , accounts.type as account_type
from transactions_int as transactions_int
left join cleaned.categories as categories
    on transactions_int.category_id = categories.id
left join cleaned.category_groups as category_groups
    on categories.category_group_id = category_groups.id
left join cleaned.accounts as accounts
    on transactions_int.account_id = accounts.id
order by transactions_int.transaction_date desc
