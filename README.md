
# DQ Sentinel — Tailwyndz Propel Assignment

This repository contains a small Data Quality (DQ) sentinel for weekly retail time-series data.  
It implements a set of checks (schema, duplicates/timezone anomalies, backfill detection, level shifts, unit/price mix-ups, and basic cadence checks), produces per-file JSON outputs and a summary CSV, and includes a small CI pipeline to run the checks automatically.

---
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


## Reproducible environment

Use a Python virtual environment and install dependencies from `requirements.txt`.

Windows PowerShell:
```powershell
python -m venv .venv
.\.venv\Scripts\Activate
python -m pip install --upgrade pip
pip install -r requirements.txt
