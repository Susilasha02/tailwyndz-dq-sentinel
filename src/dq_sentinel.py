#!/usr/bin/env python3
"""
dq_sentinel.py

Command-line runner that executes the checks implemented in checks.py
against CSV files in a data folder and writes per-file JSON reports and a summary CSV.

Usage:
    python src/dq_sentinel.py --data-dir ./data --out-dir ./reports --calendar ./data/calendar.csv --promos ./data/promos.csv
"""

import argparse
import json
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime
from checks import (
    normalize_df,
    check_schema,
    check_duplicates,
    check_date_continuity,
    detect_level_shift,
    detect_unit_price_mixup,
    detect_partial_backfill,
    detect_timezone_shift,
    detect_schema_versioning,
    detect_seasonality_break,
    promo_calendar_diagnostics,
    expected_schema_v1,
)

# Default thresholds (can be extended to read from config or CLI)
DEFAULTS = {
    "partial_backfill_pct_threshold": 0.20,  # 20% rows backfilled => blocking
    "duplicate_block_threshold": 1,  # any duplicates => blocking
    "level_shift_groups_threshold": 1,  # any group with level shift => blocking
}


def analyze_file(path: Path, out_dir: Path, promos_df=None, calendar_df=None) -> dict:
    result = {"file": path.name, "analyzed_at": datetime.utcnow().isoformat() + "Z"}
    try:
        df = pd.read_csv(path)
    except Exception as e:
        result["error"] = f"read_error: {e}"
        result["blocking"] = "FAIL"
        result["blocking_reasons"] = ["read_error"]
        return result
    # normalize
    df = normalize_df(df)
    # checks
    result["schema"] = check_schema(df, expected=expected_schema_v1())
    result["schema_versioning"] = detect_schema_versioning(df, expected=expected_schema_v1())
    result["duplicates"] = check_duplicates(df)
    result["continuity"] = check_date_continuity(df)
    result["level_shift"] = detect_level_shift(df)
    result["unit_price_mixup"] = detect_unit_price_mixup(df)
    result["partial_backfill"] = detect_partial_backfill(df)
    result["tz_shift"] = detect_timezone_shift(df)
    result["seasonality"] = detect_seasonality_break(df)
    result["promo_calendar"] = promo_calendar_diagnostics(df, promos_df, calendar_df)

    # Blocking logic (conservative and explainable)
    blocking_reasons = []
    # schema missing columns -> fail
    if result["schema"].get("missing_columns"):
        blocking_reasons.append("missing_schema_columns")
    # duplicates
    dup_count = result["duplicates"].get("duplicate_count")
    if dup_count is not None and dup_count >= DEFAULTS["duplicate_block_threshold"]:
        blocking_reasons.append(f"duplicates:{dup_count}")
    # partial backfill
    pct_backfilled = result["partial_backfill"].get("pct_backfilled")
    if pct_backfilled is not None and pct_backfilled > DEFAULTS["partial_backfill_pct_threshold"]:
        blocking_reasons.append(f"partial_backfill_pct:{round(pct_backfilled,3)}")
    # unit/price mixup
    if result["unit_price_mixup"].get("suspected_unit_price_mixup"):
        blocking_reasons.append("unit_price_mixup_suspected")
    # level shift groups
    if result["level_shift"].get("groups_with_level_shift", 0) >= DEFAULTS["level_shift_groups_threshold"]:
        blocking_reasons.append(f"level_shift_groups:{result['level_shift'].get('groups_with_level_shift')}")
    # tz suspicious
    if result["tz_shift"].get("suspicious"):
        blocking_reasons.append("timezone_anomaly_or_dup_pk")
    # finalize
    result["blocking_reasons"] = blocking_reasons
    result["blocking"] = "FAIL" if len(blocking_reasons) > 0 else "PASS"
    # write JSON per-file
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / f"{path.stem}_dq_report.json"
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str)
    return result


def load_optional_csv(path: str):
    if path is None:
        return None
    p = Path(path)
    if not p.exists():
        print(f"Warning: optional file {path} not found, skipping.")
        return None
    try:
        df = pd.read_csv(p)
        return df
    except Exception as e:
        print(f"Warning: failed to read {path}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Run DQ sentinel on a folder of CSV files.")
    parser.add_argument("--data-dir", required=True, help="Directory containing CSV files to analyze.")
    parser.add_argument("--out-dir", default="./reports", help="Directory to write JSON reports + summary CSV.")
    parser.add_argument("--calendar", default=None, help="Optional calendar CSV path.")
    parser.add_argument("--promos", default=None, help="Optional promos CSV path.")
    parser.add_argument("--pattern", default="sales_weekly*.csv", help="Glob pattern for CSVs to analyze.")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    out_dir = Path(args.out_dir)
    if not data_dir.exists():
        print(f"ERROR: data-dir {data_dir} does not exist.", file=sys.stderr)
        sys.exit(2)

    promos_df = load_optional_csv(args.promos)
    calendar_df = load_optional_csv(args.calendar)

    csv_files = sorted(data_dir.glob(args.pattern))
    if not csv_files:
        print(f"No files found matching pattern {args.pattern} in {data_dir}")
        sys.exit(3)

    summary_rows = []
    any_fail = False
    for f in csv_files:
        print(f"Analyzing {f.name} ...")
        r = analyze_file(f, out_dir, promos_df=promos_df, calendar_df=calendar_df)
        # build summary row
        summary_rows.append(
            {
                "file": r.get("file"),
                "blocking": r.get("blocking"),
                "blocking_reasons": ";".join(r.get("blocking_reasons") or []),
                "duplicate_count": (r.get("duplicates") or {}).get("duplicate_count"),
                "pct_backfilled": (r.get("partial_backfill") or {}).get("pct_backfilled"),
                "suspected_unit_price_mixup": (r.get("unit_price_mixup") or {}).get("suspected_unit_price_mixup"),
                "level_shift_groups": (r.get("level_shift") or {}).get("groups_with_level_shift"),
            }
        )
        if r.get("blocking") == "FAIL":
            any_fail = True

    # write summary CSV
    summary_df = pd.DataFrame(summary_rows)
    out_dir.mkdir(parents=True, exist_ok=True)
    summary_csv = out_dir / "summary_report.csv"
    summary_df.to_csv(summary_csv, index=False)
    print(f"Wrote summary to {summary_csv}")

    # exit non-zero if any file fails (useful for CI).
    if any_fail:
        print("One or more files are blocking (FAIL). Exiting with code 1.")
        sys.exit(1)
    else:
        print("All files passed checks. Exiting with code 0.")
        sys.exit(0)


if __name__ == "__main__":
    main()
