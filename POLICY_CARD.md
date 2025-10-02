# Policy Card — Anomaly Scoring (DQ Sentinel)

## Component purpose / scope
This component assigns an overall status (Red / Amber / Green) to a dataset run based on the presence and severity of data quality issues. It is intended for weekly retail time-series CSV files containing columns like `week_start`, `sku_id`, `store_id`, `units`, `price`, `inventory_on_hand`, `load_ts`, and `currency`.

## Inputs
- Per-file DQ results (per-check JSONs).
- Summary table `summary_report.csv` with flags such as `blocking`, `duplicate_count`, `pct_backfilled`, `suspected_unit_price_mixup`, and `level_shift_groups`.

## Scoring logic (how statuses are assigned)
- **Red**: any file has `blocking == FAIL` (critical issues: missing required schema columns or duplicates that invalidate the PK).
- **Amber**: no blocking fails, but one or more non-blocking but significant issues:
  - duplicates > 0 (but deduped successfully),
  - `pct_backfilled` > 0 (partial backfill),
  - `suspected_unit_price_mixup` flagged,
  - `level_shift_groups` > 0 (significant level shifts detected)
- **Green**: no issues found (all files `PASS` or only minor/expected warnings).

## Assumptions
- The canonical primary key is `(week_start, sku_id, store_id)`.
- `week_start` is weekly and consistent; cadence checks look for consistent 7-day spacing.
- `load_ts` is reliable for resolving duplicates when present.
- Backfill detection is approximate: we flag when recent historic weeks have been populated to a suspicious degree.

## Risks / failure modes
- **False positives**: legitimate business events (massive promo campaigns, store openings/closings) may look like level shifts.
- **False negatives**: subtle data drift might not trigger current heuristics.
- **Interpretation risk**: an Amber status requires human review — the system does not automatically alter production models.

## Operational guidance
- Red status → stop automated downstream model re-train and alert a data steward.
- Amber status → notify a data steward for review; consider running more in-depth diagnostics (change point detection).
- Green status → safe for automated downstream consumption (but continue periodic monitoring).

## Governance / ownership
- Owner: Data engineering / analytics team.
- Review cadence: weekly checks with periodic review after major schema changes.

## Extensibility
- Add confidence scores, add changepoint analysis, and store historical DQ metrics for trend monitoring.
