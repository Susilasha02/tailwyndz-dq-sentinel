from typing import Dict, Any, Optional, Tuple, List
import pandas as pd
import numpy as np
from dateutil import parser
import math


def expected_schema_v1() -> List[str]:
    return [
        "week_start",
        "sku_id",
        "store_id",
        "units",
        "price",
        "inventory_on_hand",
        "currency",
        "load_ts",
        "source_file",
    ]


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    # Make a shallow copy and normalize column names and basic types
    df = df.copy()
    # strip column whitespace
    df.columns = [c.strip() for c in df.columns]
    # try to coerce numeric columns
    if "units" in df.columns:
        df["units"] = pd.to_numeric(df["units"], errors="coerce")
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
    # parse datetimes
    if "week_start" in df.columns:
        df["week_start"] = pd.to_datetime(df["week_start"], errors="coerce")
    if "load_ts" in df.columns:
        # preserve raw string too
        df["load_ts_parsed"] = pd.to_datetime(df["load_ts"], errors="coerce")
    return df


def check_schema(df: pd.DataFrame, expected: Optional[List[str]] = None) -> Dict[str, Any]:
    if expected is None:
        expected = expected_schema_v1()
    cols = list(df.columns)
    missing = [c for c in expected if c not in cols]
    extra = [c for c in cols if c not in expected]
    return {"missing_columns": missing, "extra_columns": extra, "schema_ok": len(missing) == 0}


def check_duplicates(df: pd.DataFrame, pk: Optional[List[str]] = None) -> Dict[str, Any]:
    if pk is None:
        pk = ["week_start", "sku_id", "store_id"]
    for c in pk:
        if c not in df.columns:
            return {"status": "pk_column_missing", "pk": pk, "duplicate_count": None, "sample": []}
    dup_mask = df.duplicated(subset=pk, keep=False)
    dup_count = int(dup_mask.sum())
    sample = df[dup_mask].head(10).to_dict(orient="records")
    return {"status": "ok", "pk": pk, "duplicate_count": dup_count, "sample": sample}


def check_date_continuity(
    df: pd.DataFrame, freq_days: int = 7, allowed_gaps: int = 1
) -> Dict[str, Any]:
    # Expect weekly cadence per sku_id, store_id
    if "week_start" not in df.columns:
        return {"status": "no_week_start_column"}
    g = df.groupby(["sku_id", "store_id"])
    groups_total = 0
    groups_with_gaps = 0
    gap_examples = []
    for (sku, store), sub in g:
        groups_total += 1
        weeks = sub["week_start"].dropna().sort_values().drop_duplicates()
        if len(weeks) < 2:
            continue
        diffs_days = weeks.diff().dt.days.dropna()
        # count diffs not equal to expected freq
        bad = (diffs_days != freq_days) & (diffs_days != 0)
        bad_count = int(bad.sum())
        if bad_count > allowed_gaps:
            groups_with_gaps += 1
            # capture a small example for reporting
            gap_examples.append({"sku_id": sku, "store_id": store, "bad_diffs_sample": diffs_days.head(5).tolist()})
    return {
        "status": "ok",
        "groups_total": int(groups_total),
        "groups_with_gaps": int(groups_with_gaps),
        "gap_examples": gap_examples[:10],
    }


def detect_level_shift(
    df: pd.DataFrame, window: int = 8, z_threshold: float = 3.0
) -> Dict[str, Any]:
    """Detect groups where the mean in the final window differs from the initial window by > z_threshold * std."""
    if "units" not in df.columns or "week_start" not in df.columns:
        return {"status": "columns_missing"}
    df = df.copy()
    df = df.sort_values(["sku_id", "store_id", "week_start"])
    groups = df.groupby(["sku_id", "store_id"])
    groups_tested = 0
    groups_with_shift = 0
    examples = []
    for (sku, store), sub in groups:
        units = sub["units"].dropna().astype(float)
        if len(units) < window * 2:
            continue
        groups_tested += 1
        first_mean = units.iloc[:window].mean()
        last_mean = units.iloc[-window:].mean()
        pooled_std = units.std(ddof=1)
        if pooled_std == 0 or math.isnan(pooled_std):
            continue
        z = abs(last_mean - first_mean) / pooled_std
        if z >= z_threshold:
            groups_with_shift += 1
            examples.append({"sku_id": sku, "store_id": store, "z_score": float(z), "first_mean": float(first_mean), "last_mean": float(last_mean)})
    return {
        "status": "ok",
        "groups_tested": groups_tested,
        "groups_with_level_shift": groups_with_shift,
        "examples": examples[:10],
    }


def detect_unit_price_mixup(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Heuristic: if price median is very small (< 1.5) while units median is relatively large,
    suspect a swap. Also check extreme ratios.
    """
    if "price" not in df.columns or "units" not in df.columns:
        return {"status": "columns_missing"}
    price_median = float(pd.to_numeric(df["price"], errors="coerce").median(skipna=True) or 0.0)
    units_median = float(pd.to_numeric(df["units"], errors="coerce").median(skipna=True) or 0.0)
    ratio = None
    suspected = False
    hint = ""
    if price_median == 0:
        ratio = None
    else:
        ratio = units_median / price_median if price_median != 0 else None
    # heuristics
    if price_median < 1.5 and units_median > 10:
        suspected = True
        hint = "Median price < 1.5 while median units > 10 — possible price/units mixup."
    # additional check: price has many integer-like values identical to typical units
    if not suspected:
        # check many price values > 10 but have small decimals?
        price_mode = pd.to_numeric(df["price"], errors="coerce").dropna()
        if len(price_mode) > 0:
            if (price_mode % 1 == 0).mean() > 0.9 and price_mode.median() > 5 and df["units"].median() < 2:
                suspected = True
                hint = "Price values look integer-like and larger than typical units; possible swap."
    return {
        "status": "ok",
        "price_median": price_median,
        "units_median": units_median,
        "ratio_units_to_price": ratio,
        "suspected_unit_price_mixup": bool(suspected),
        "hint": hint,
    }


def detect_partial_backfill(df: pd.DataFrame, threshold_days: int = 30) -> Dict[str, Any]:
    """
    Heuristic: if many rows have load_ts >> week_start (e.g., loaded more than threshold_days after week),
    likely partial backfill.
    """
    if "week_start" not in df.columns or "load_ts_parsed" not in df.columns:
        return {"status": "columns_missing"}
    df = df.copy()
    df["delta_days"] = (df["load_ts_parsed"] - df["week_start"]).dt.days
    # rows with load_ts parsed NaT will be ignored in this metric
    valid = df["delta_days"].dropna()
    if len(valid) == 0:
        return {"status": "no_load_ts_parsed"}
    backfilled_count = int((valid > threshold_days).sum())
    pct_backfilled = float(backfilled_count) / float(len(valid))
    hint = ""
    if pct_backfilled > 0.05:
        hint = f"{round(pct_backfilled*100,2)}% of rows were loaded > {threshold_days} days after the week_start — partial backfill likely."
    return {"status": "ok", "count_checked": int(len(valid)), "count_backfilled": backfilled_count, "pct_backfilled": pct_backfilled, "hint": hint}


def detect_timezone_shift(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Look for timezone offsets in load_ts strings and for duplicate primary keys that differ by load_ts.
    """
    if "load_ts" not in df.columns:
        return {"status": "no_load_ts_column"}
    tz_variants = []
    # try to find +HH:MM or -HH:MM or Z
    tz_series = df["load_ts"].astype(str).str.extract(r'([+-]\d{2}:?\d{2}|Z)')[0].dropna().unique().tolist()
    if tz_series:
        tz_variants = tz_series
    # duplicate primary keys
    dup_pk = 0
    if set(["week_start", "sku_id", "store_id"]).issubset(df.columns):
        dup_pk = int(df.duplicated(subset=["week_start", "sku_id", "store_id"], keep=False).sum())
    suspicious = len(tz_variants) > 1 or dup_pk > 0
    hint = ""
    if len(tz_variants) > 1:
        hint += f"Found multiple timezone patterns in load_ts: {tz_variants}. "
    if dup_pk > 0:
        hint += "Found duplicate primary keys which may be caused by timezone-normalization inconsistencies. "
    return {"status": "ok", "tz_variants": tz_variants, "duplicate_pk_count": dup_pk, "suspicious": suspicious, "hint": hint}


def detect_schema_versioning(df: pd.DataFrame, expected: Optional[List[str]] = None) -> Dict[str, Any]:
    if expected is None:
        expected = expected_schema_v1()
    missing = [c for c in expected if c not in df.columns]
    extra = [c for c in df.columns if c not in expected]
    changed = len(missing) > 0 or len(extra) > 0
    hint = ""
    if changed:
        hint = f"Missing columns: {missing}. Extra columns: {extra}."
    return {"status": "ok", "schema_changed": changed, "missing": missing, "extra": extra, "hint": hint}


def detect_seasonality_break(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute simple autocorrelations on the aggregated weekly series to detect if
    yearly seasonality (lag ~52 weeks) is present or broken.
    Returns ACF at lag 1 and lag 52 (if enough data).
    """
    if "week_start" not in df.columns or "units" not in df.columns:
        return {"status": "missing_columns"}
    agg = df.groupby("week_start")["units"].sum().sort_index()
    n = len(agg)
    if n < 24:
        return {"status": "not_enough_history", "n_weeks": n}
    acf1 = float(agg.autocorr(lag=1)) if n > 1 else None
    acf52 = float(agg.autocorr(lag=52)) if n > 52 else None
    # no historic baseline, so just return the metrics; caller can decide what threshold to use
    return {"status": "ok", "n_weeks": n, "acf_lag1": acf1, "acf_lag52": acf52}


def promo_calendar_diagnostics(
    df: pd.DataFrame, promos: Optional[pd.DataFrame], calendar: Optional[pd.DataFrame]
) -> Dict[str, Any]:
    """
    Provide hints if spikes or drops correlate with promotions or calendar events.
    Very lightweight: we check whether weeks with large relative change coincide with a promo flag.
    Expects promos to have columns like 'week_start','sku_id','store_id','promo_flag' or similar.
    """
    hints = []
    if promos is None and calendar is None:
        return {"status": "no_data", "hints": hints}
    # aggregate weekly units
    if "week_start" not in df.columns or "units" not in df.columns:
        return {"status": "missing_columns", "hints": hints}
    agg = df.groupby("week_start")["units"].sum().sort_index()
    if len(agg) < 4:
        return {"status": "not_enough_history", "hints": hints}
    pct_change = agg.pct_change().abs().fillna(0)
    big_changes = pct_change[pct_change > 0.5]  # >50% change week to week
    if len(big_changes) == 0:
        return {"status": "no_big_changes", "hints": hints}
    # check for promos
    if promos is not None and set(["week_start"]).issubset(promos.columns):
        # convert promos week_start
        promos = promos.copy()
        promos["week_start"] = pd.to_datetime(promos["week_start"], errors="coerce")
        promo_weeks = promos["week_start"].dropna().unique().tolist()
        overlaps = [str(w.date()) for w in big_changes.index if pd.to_datetime(w).date() in [pd.to_datetime(x).date() for x in promo_weeks]]
        if overlaps:
            hints.append(f"Large weekly changes on weeks {overlaps} overlap with promo weeks.")
    if calendar is not None and set(["week_start"]).issubset(calendar.columns):
        calendar = calendar.copy()
        calendar["week_start"] = pd.to_datetime(calendar["week_start"], errors="coerce")
        event_weeks = calendar["week_start"].dropna().unique().tolist()
        overlaps = [str(w.date()) for w in big_changes.index if pd.to_datetime(w).date() in [pd.to_datetime(x).date() for x in event_weeks]]
        if overlaps:
            hints.append(f"Large weekly changes on weeks {overlaps} overlap with calendar event weeks.")
    if not hints:
        hints.append("Large weekly changes detected but no matching promo/calendar rows found.")
    return {"status": "ok", "n_big_changes": int(len(big_changes)), "big_change_weeks": [str(d.date()) for d in big_changes.index], "hints": hints}
