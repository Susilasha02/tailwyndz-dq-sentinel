This pack contains a **clean baseline** and multiple **faulty variants** to test a Data Quality Sentinel for weekly retail time-series.

## Files

- `sales_weekly.csv` — clean baseline (80 Mondays from 2023-01-02 to 2024-07-08, 5 SKUs × 3 stores)
  - Columns: `week_start, sku_id, store_id, units, price, inventory_on_hand, currency, load_ts, source_file`
- `calendar.csv` — weekly calendar with `holiday_flag` and `fiscal_week`
- `promos.csv` — promo windows (`discount`, `bundle`, `display`)

### Faulty variants
- `sales_weekly_dupes.csv`
- `sales_weekly_schema_v2.csv`
- `sales_weekly_partial_backfill.csv`
- `sales_weekly_tz_shift.csv`
- `sales_weekly_level_shift.csv`
- `sales_weekly_unit_mixup.csv`
- `sales_weekly_season_break.csv`

## Primary key
Use `(week_start, sku_id, store_id)` as the natural key.


## CI/CD friendly outputs (what your checker might emit)
- Exit code: `0` (green), `2` (amber), `3` (red)
- Artifacts: `dq_report.html`, `dq_findings.csv`, `dq_summary.json`

