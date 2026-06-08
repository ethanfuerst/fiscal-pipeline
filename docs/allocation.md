# Income allocation — how it works

This document explains how the warehouse turns income into per-bucket targets,
measures adherence, and patches overages with extra income. It covers the chain
of `core` models that implement the financial plan's 50/30/15/5 split (§1–§8).

All figures below are formulas and column names — no dollar values. Targets apply
to **allocatable income** (salary after estimated tax and HSA), not gross salary.

## The buckets

Allocatable income is split four ways. Two buckets are "less is good" (spend at or
under target) and two are "more is good" (save/invest at or above target):

| Bucket | Share | Direction | On plan when |
| --- | --- | --- | --- |
| Needs | 50% | less-is-good | projected spend ≤ target |
| Wants | 30% | less-is-good | projected spend ≤ target |
| Investments | 15% | more-is-good | actual contributions ≥ target |
| Savings | 5% | more-is-good | net saved ≥ target |

## Model chain

The models build in dependency order (each depends only on the ones above it):

1. **`core.yearly_income_derivation`** (§1) — per year: `salary`, `estimated_tax`,
   `hsa`, `allocatable_income`, the four bucket targets (`needs_target`,
   `wants_target`, `investments_target`, `savings_target` = `allocatable_income`
   × 0.50 / 0.30 / 0.15 / 0.05), and `extra_income`.
2. **`core.yearly_bucket_adherence`** (Needs/Wants) — per `(year, bucket)`:
   `target`, `projected` spend, `overage_pct`, `on_plan_flag` (less-is-good).
3. **`core.yearly_savings_adherence`** (Savings) — per year: `savings_budgeted`,
   `savings_spent`, `target`, the **pre-coverage** `projected` net saved
   (`extra_covered = 0`), and `on_plan_flag` (more-is-good).
4. **`core.yearly_investment_contributions`** (Investments) — per year: 401(k) and
   taxable brokerage actuals, the salary-based target split (50/50 with employee
   elective-limit spillover), and remaining-to-target.
5. **`core.yearly_extra_income_allocation`** (§8) — the extra-income waterfall
   (below). Reads models 1–3.
6. **`core.yearly_investment_contributions`** also reads model 5 to wire the
   investment surplus back in (the augmented split). This back-edge is acyclic
   because the allocation model never reads the contributions model.

### `projected` vs actual

`projected` (in `yearly_bucket_adherence`, and the analogous net-saved figure in
`yearly_savings_adherence`) is the full-year actual for past years, but for the
current in-progress year it is the year-to-date figure **extrapolated to a
full-year estimate** so it stays comparable to the full-year target. The
`is_extrapolated` flag is `true` only for that current-year row. The value is
called "projected" precisely because, for the live year, it is a forecast — not
the raw year-to-date actual.

### `extra_income` (the thing being allocated)

`extra_income` is the per-year sum of Ready-to-Assign (`category_group_name_mapping
= 'Income'`) net inflows in `core.daily_ledger` that are **not** regular-salary
paychecks. It is net-to-account one-off money — bonus, severance, PTO payout,
refunds, interest, cashback, side deposits — identified by the absence of a
regular-salary paystub link (`coalesce(earnings_salary_usd, 0) = 0`), not by payee
text. It is actual year-to-date only (never extrapolated) and does **not** feed
`allocatable_income`.

## The §8 waterfall

`extra_income` is applied as a patch in a fixed priority order. Each step consumes
what it can, and the remainder flows to the next step. Let `extra =
coalesce(extra_income, 0)`.

1. **Needs overage** — `needs_overage = max(0, needs_projected − needs_target)`;
   `extra_income_used_for_needs_overage = min(extra, needs_overage)`.
2. **Wants overage** — with `remaining_after_needs = extra −
   extra_income_used_for_needs_overage`:
   `extra_income_used_for_wants_overage = min(remaining_after_needs, max(0,
   wants_projected − wants_target))`.
3. **Savings coverage** — with `remaining_after_wants = remaining_after_needs −
   extra_income_used_for_wants_overage`:
   `extra_income_used_for_savings_coverage = min(remaining_after_wants,
   savings_spent)`. Spend-only — it backfills money withdrawn from Savings, caps at
   `savings_spent`, and never funds new savings or raises the target.
4. **Investments surplus** — whatever is left:
   `extra_income_surplus_to_investments = remaining_after_wants −
   extra_income_used_for_savings_coverage`.

By construction the four outputs always sum to `extra_income` (the surplus absorbs
the remainder). This is enforced by the
`yearly_extra_income_allocation_conservation` audit (tolerance 0.02).

> **Priority order is Needs → Wants.** The original ticket listed Wants-first; this
> was flipped to Needs-first. Only the two `least()` steps and their `remaining_*`
> carry-overs would swap if revisited.

### What the patch does NOT do

- **Targets are never inflated.** The overage patches record how much extra income
  absorbed an overage, but `core.yearly_bucket_adherence` still flags the raw
  overage. No silent target inflation or double-counting.
- **Savings stays pre-coverage upstream.** `core.yearly_savings_adherence` remains
  the pre-coverage floor (`extra_covered = 0`). The coverage-adjusted view is
  produced downstream in the allocation model, never written back — this keeps the
  dependency acyclic (savings → allocation, never the reverse).

## Coverage-adjusted savings

The allocation model produces the savings adherence figure *after* coverage:

- `net_saved_after_coverage = savings_budgeted − max(0, savings_spent −
  extra_income_used_for_savings_coverage)`.
- `savings_on_plan_after_coverage = net_saved_after_coverage ≥ savings_target`
  (null when `savings_target` is null).

Coverage can flip a year from off-plan to on-plan when covered withdrawals clear
the savings goal.

## Investment surplus wiring

`extra_income_surplus_to_investments` flows back into
`core.yearly_investment_contributions`, where it augments the salary-based target
and is re-split with the same employee-limit spillover. The salary-only columns are
left intact; the surplus-inclusive view is added alongside:

- `investments_target_with_surplus_usd = investments_target +
  extra_income_surplus_to_investments`.
- `target_401k_split_with_surplus_usd` / `target_taxable_split_with_surplus_usd` —
  50/50 of the augmented target, 401(k) capped at the employee elective-deferral
  limit with the remainder spilling to taxable.
- `investments_remaining_with_surplus_usd = max(0,
  investments_target_with_surplus_usd − investments_actual_usd)`.

## Per-bucket on-plan-after-allocation flags

Every bucket exposes a flag for whether it is on plan **after** the extra-income
patch. They are split across two models by the acyclicity constraint (the
Investments flag needs actuals, which live in the contributions model):

| Bucket | Flag | Model | Definition |
| --- | --- | --- | --- |
| Needs | `needs_on_plan_after_allocation` | `yearly_extra_income_allocation` | `(needs_projected − extra_income_used_for_needs_overage) ≤ needs_target` |
| Wants | `wants_on_plan_after_allocation` | `yearly_extra_income_allocation` | `(wants_projected − extra_income_used_for_wants_overage) ≤ wants_target` |
| Savings | `savings_on_plan_after_coverage` | `yearly_extra_income_allocation` | `net_saved_after_coverage ≥ savings_target` |
| Investments | `investments_on_plan_after_surplus_flag` | `yearly_investment_contributions` | `investments_actual_usd ≥ investments_target_with_surplus_usd` |

Needs/Wants flags are null when the bucket has no target (no data); Savings and
Investments flags are null when their target is null.

`core.yearly_extra_income_allocation` is kept lean — it emits only its own derived
columns (`extra_income`, the four patch outputs, `net_saved_after_coverage`, the
three flags, `is_extrapolated`) and does not re-expose the upstream
projected/target/spent components. Its two on-plan-consistency audits therefore
join the upstream adherence models to re-derive the flags.

## Audits

Each derived relationship has a consistency audit (sqlmesh fails the plan if one
breaks):

- `yearly_income_derivation_bucket_targets_sum` — the four targets sum to
  `allocatable_income`.
- `yearly_extra_income_allocation_conservation` — the four patch outputs sum to
  `extra_income`.
- `yearly_extra_income_allocation_savings_on_plan_consistency` — savings flag
  matches `net_saved_after_coverage ≥ savings_target` (joins
  `yearly_savings_adherence`).
- `yearly_extra_income_allocation_overage_on_plan_consistency` — Needs/Wants flags
  match their definitions (joins `yearly_bucket_adherence`).
- `yearly_investment_contributions_split_targets_sum` /
  `yearly_investment_contributions_surplus_split_targets_sum` — the base and
  surplus-inclusive 401(k)/taxable splits each sum to their target.
- `yearly_investment_contributions_surplus_on_plan_consistency` — Investments flag
  matches `investments_actual_usd ≥ investments_target_with_surplus_usd`.
