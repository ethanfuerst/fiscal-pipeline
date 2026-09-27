# Adherence Waterfall Flowchart

This shows how the current adherence waterfall sheet thinks about income, spending, savings, buffer cash, emergency fund cash, and HSA reimbursement value.

## 50/30/15/5 Plan

```mermaid
flowchart TB
    income[Net Income]

    subgraph target_layer[Target Layer]
        needs_target[Needs Target]
        wants_target[Wants Target]
        savings_target[Savings Target]
        investments_target[Investments Target]
    end

    subgraph actual_layer[Spend and Saved Layer]
        needs_spend[Needs Spend]
        wants_spend[Wants Spend]
        savings_saved[Savings Saved]
        investments_saved[Investments Saved]
    end

    subgraph surplus_layer[Surplus Layer]
        needs_surplus[Needs Surplus]
        wants_surplus[Wants Surplus]
        savings_surplus[Savings Surplus]
        investments_surplus[Investments Surplus]
    end

    income --> needs_target
    income --> wants_target
    income --> savings_target
    income --> investments_target

    needs_target --> needs_spend
    wants_target --> wants_spend
    savings_target --> savings_saved
    investments_target --> investments_saved

    needs_target --> needs_surplus
    needs_spend --> needs_surplus
    wants_target --> wants_surplus
    wants_spend --> wants_surplus
    savings_target --> savings_surplus
    savings_saved --> savings_surplus
    investments_target --> investments_surplus
    investments_saved --> investments_surplus

    needs_surplus --> overflow[Overflow Dollars]
    wants_surplus --> overflow
    savings_surplus --> overflow
    investments_surplus --> overflow
```

## Buffer And Reserves

```mermaid
flowchart TB
    placeholder_buffer[Placeholder Zero] --> buffer_balance[Buffer Balance]
    overflow --> used_from_buffer[Used From Buffer]
    buffer_balance --> used_from_buffer

    buffer_balance --> buffer_surplus[Buffer Surplus]
    spend_average[Needs/Wants Spend Average] --> buffer_target[Buffer Target]
    buffer_target --> buffer_surplus

    ef_balance[Emergency Fund Balance] --> ef_surplus[Emergency Fund Surplus]
    ef_target[Emergency Fund Target] --> ef_surplus

    buffer_surplus --> reserve_surplus[Reserve Surplus]
    ef_surplus --> reserve_surplus
```

## Savings Details

```mermaid
flowchart TB
    savings_saved[Savings Saved] --> savings_net[Savings Net]
    savings_spend[Savings + Emergency Fund Spend] --> savings_net

    savings_net --> rolling_savings[Rolling Savings Balance]
    rolling_savings --> other_savings[Other Savings Balance]
    ef_balance[Emergency Fund Balance] --> other_savings

```

## Current Sheet Logic

| Sheet column | Meaning |
| --- | --- |
| `Net Income` | Income deposited to budget accounts for the period. |
| `Needs Target` | 50% of Net Income. |
| `Needs Spend` | Actual Needs spending, shown as a negative number. |
| `Needs Surplus` | Needs Target plus Needs Spend. Positive means under target; negative means over target. |
| `Wants Target` | 30% of Net Income. |
| `Wants Spend` | Actual Wants spending, shown as a negative number. |
| `Wants Surplus` | Wants Target plus Wants Spend. Positive means under target; negative means over target. |
| `Savings Target` | 5% of Net Income. |
| `Savings Saved` | Assignments to Savings and Emergency Fund categories, shown as a negative number to compare against the target. |
| `Savings Surplus` | Savings Target plus Savings Saved. Positive means under the target; negative means more than target was assigned. |
| `Savings Net` | Savings and Emergency Fund assignments minus spending from those categories. |
| `Rolling Savings Balance` | Month-end Savings plus Emergency Fund balance in YNAB. |
| `Investments Target` | 15% of Net Income. |
| `Investments Saved` | Assignments to taxable investment categories, shown as a negative number to compare against the target. |
| `Investments Surplus` | Investments Target plus Investments Saved. Positive means under target; negative means more than target was assigned. |
| `Overflow Dollars` | Net Income minus current Needs/Wants spend and saved dollars. |
| `Buffer Balance` | Temporarily set to zero while buffer balance logic is being reconsidered. |
| `Buffer Target` | Average Needs/Wants spend from the current year once six complete months exist; otherwise the trailing six complete months. |
| `Buffer Surplus` | Temporarily set to zero while buffer surplus logic is being reconsidered. |
| `Emergency Fund Target` | Buffer Target multiplied by 3. |
| `Emergency Fund Balance` | Cash emergency fund balance in YNAB. |
| `Emergency Fund Surplus` | Emergency Fund Balance minus Emergency Fund Target. HSA reimbursement value is not included. |
| `Reserve Surplus` | Buffer Surplus plus Emergency Fund Surplus. This is the combined cash reserve amount to drive toward zero. |
| `Other Savings Balance` | Savings category balance outside the Emergency Fund cash balance. |
| `Used From Buffer` | Negative monthly overflow covered by current Needs/Wants available. |

## Formula Summary

```text
Needs Surplus = Needs Target + Needs Spend
Wants Surplus = Wants Target + Wants Spend
Savings Surplus = Savings Target + Savings Saved
Investments Surplus = Investments Target + Investments Saved

Overflow Dollars =
  Net Income
  - current Needs/Wants spending
  - saved dollars

Buffer Balance = 0 while buffer logic is being reconsidered
Buffer Surplus = 0 while buffer logic is being reconsidered
Emergency Fund Target = Buffer Target * 3
Emergency Fund Surplus = Emergency Fund Balance - Emergency Fund Target
Reserve Surplus = Buffer Surplus + Emergency Fund Surplus

```
