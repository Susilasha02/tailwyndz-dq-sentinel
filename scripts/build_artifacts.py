#!/usr/bin/env python3
"""
scripts/build_artifacts.py

Generate deliverable artifacts:
 - dq_findings.csv (tabular findings / copy of summary)
 - dq_summary.json (status: red/amber/green plus metadata)
 - cleaned_timeseries.csv (concatenated cleaned files, deduped)
 - reports/plots/*.png (missingness, cadence, simple level-shift distribution)

Usage:
    python scripts/build_artifacts.py
"""
from pathlib import Path
import pandas as pd
import json
import sys
import zipfile
import matplotlib.pyplot as plt
import numpy as np

ROOT = Path('.')
REPORTS_DIR = ROOT / 'reports'
CI_DIR = REPORTS_DIR / 'ci'
PLOTS_DIR = REPORTS_DIR / 'plots'
CLEANED_DIR = ROOT / 'data' / 'cleaned'
DQ_FINDINGS = REPORTS_DIR / 'dq_findings.csv'
DQ_SUMMARY = REPORTS_DIR / 'dq_summary.json'
CLEANED_OUT = ROOT / 'data' / 'cleaned_timeseries.csv'

PLOTS_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

def read_summary():
    """Try to read summary_report.csv from CI dir or extract from dq-reports.zip."""
    possible = [
        CI_DIR / 'summary_report.csv',
        REPORTS_DIR / 'summary_report.csv',
        ROOT / 'dq-reports.zip',
    ]
    for p in possible:
        if p.exists():
            if p.suffix == '.zip':
                # try to extract summary_report.csv from the zip
                with zipfile.ZipFile(p, 'r') as z:
                    for name in z.namelist():
                        if name.endswith('summary_report.csv'):
                            print(f"Extracting {name} from {p}")
                            z.extract(name, path=REPORTS_DIR)
                            # returned path
                            return REPORTS_DIR / name
                continue
            else:
                return p
    return None

def build_dq_findings(summary_path: Path):
    df = pd.read_csv(summary_path)
    # normalize column names
    df.columns = [c.strip() for c in df.columns]
    # write findings CSV
    DQ_FINDINGS.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(DQ_FINDINGS, index=False)
    print(f"Wrote dq_findings -> {DQ_FINDINGS}")
    return df

def compute_overall_status(df: pd.DataFrame):
    """
    Determine overall status:
      - 'red'   : any blocking == FAIL
      - 'amber' : no blocking FAILs but at least one notable non-blocking issue
                 (duplicate_count > 0, pct_backfilled > 0, suspected_unit_price_mixup True, level_shift_groups > 0)
      - 'green' : none of the above
    """
    # RED if any blocking FAIL
    if 'blocking' in df.columns:
        blocking_mask = df['blocking'].astype(str).str.upper() == 'FAIL'
        if blocking_mask.any():
            return 'red'

    # Check amber conditions (if any file has the condition -> amber)
    amber_flag = False

    if 'duplicate_count' in df.columns:
        try:
            if (df['duplicate_count'].fillna(0).astype(float) > 0).any():
                amber_flag = True
        except Exception:
            pass

    if 'pct_backfilled' in df.columns:
        try:
            if (df['pct_backfilled'].fillna(0).astype(float) > 0).any():
                amber_flag = True
        except Exception:
            pass

    if 'suspected_unit_price_mixup' in df.columns:
        try:
            if (df['suspected_unit_price_mixup'].astype(str).str.upper() == 'TRUE').any():
                amber_flag = True
        except Exception:
            pass

    if 'level_shift_groups' in df.columns:
        try:
            if (df['level_shift_groups'].fillna(0).astype(float) > 0).any():
                amber_flag = True
        except Exception:
            pass

    return 'amber' if amber_flag else 'green'

def write_summary_json(df: pd.DataFrame):
    status = compute_overall_status(df)
    # compute counts defensively
    red_count = 0
    dup_sum = 0
    if 'blocking' in df.columns:
        red_count = int((df['blocking'].astype(str).str.upper() == 'FAIL').sum())
    if 'duplicate_count' in df.columns:
        try:
            dup_sum = int(df['duplicate_count'].fillna(0).astype(float).sum())
        except Exception:
            dup_sum = 0

    summary = {
        'status': status,
        'files_checked': int(len(df)),
        'counts': {
            'red': red_count,
            'duplicates_total': dup_sum
        }
    }
    DQ_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    with open(DQ_SUMMARY, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"Wrote dq_summary -> {DQ_SUMMARY}")
    return summary

def build_cleaned_timeseries():
    csvs = sorted(CLEANED_DIR.glob('sales_weekly*.csv'))
    if not csvs:
        print("No cleaned CSVs found in data/cleaned/. Skipping cleaned_timeseries creation.")
        return None
    parts = []
    for p in csvs:
        try:
            df = pd.read_csv(p, parse_dates=['week_start'], infer_datetime_format=True)
            df['source_file'] = p.name
            parts.append(df)
        except Exception as e:
            print(f"Warning: failed to read {p}: {e}")
    if not parts:
        print("No readable cleaned CSVs found.")
        return None
    full = pd.concat(parts, ignore_index=True)
    # Normalize column names
    full.columns = [c.strip() for c in full.columns]
    # Standard PK: week_start, sku_id, store_id
    for c in ['week_start', 'sku_id', 'store_id']:
        if c not in full.columns:
            print(f"Warning: {c} missing from combined file; leaving as-is.")
    # Drop exact duplicates
    full = full.drop_duplicates()
    # Try dedupe by PK using last load_ts if present
    if set(['week_start', 'sku_id', 'store_id']).issubset(full.columns):
        if 'load_ts' in full.columns:
            try:
                full = full.sort_values('load_ts').drop_duplicates(subset=['week_start','sku_id','store_id'], keep='last')
            except Exception:
                full = full.drop_duplicates(subset=['week_start','sku_id','store_id'], keep='last')
        else:
            full = full.drop_duplicates(subset=['week_start','sku_id','store_id'], keep='last')
    CLEANED_OUT.parent.mkdir(parents=True, exist_ok=True)
    full.to_csv(CLEANED_OUT, index=False)
    print(f"Wrote cleaned_timeseries -> {CLEANED_OUT} (rows: {len(full)})")
    return full

def plot_missingness(df, outpath: Path):
    miss = df.isna().mean().sort_values(ascending=False)
    plt.figure(figsize=(8,4))
    miss.plot.bar()
    plt.title('Fraction missing per column')
    plt.ylabel('Fraction missing')
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()
    print(f"Wrote missingness plot -> {outpath}")

def plot_cadence(full_df, outpath: Path):
    # cadence: distribution of differences in days between consecutive week_start across all (sku,store)
    if 'week_start' not in full_df.columns:
        print("week_start not in dataframe; skipping cadence plot")
        return
    full_df = full_df.sort_values('week_start')
    diffs = full_df['week_start'].dropna().sort_values().diff().dt.days.dropna()
    if diffs.empty:
        print("No diffs for cadence plot; skipping")
        return
    plt.figure(figsize=(6,4))
    plt.hist(diffs, bins=range(0,50,1))
    plt.title('Cadence: distribution of days between week_start')
    plt.xlabel('days')
    plt.ylabel('count')
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()
    print(f"Wrote cadence plot -> {outpath}")

def plot_level_shift(full_df, outpath: Path):
    # crude check: compute mean sales per file or per first/second half and plot distribution of relative shift
    if 'units' not in full_df.columns:
        print("units not in dataframe; skipping level-shift plot")
        return
    df = full_df.copy()
    try:
        df = df.sort_values('week_start')
        df['time_idx'] = (df['week_start'] - df['week_start'].min()).dt.days
        median_time = df['time_idx'].median()
        first = df[df['time_idx'] <= median_time]
        second = df[df['time_idx'] > median_time]
        grp1 = first.groupby('source_file')['units'].mean()
        grp2 = second.groupby('source_file')['units'].mean()
        joined = pd.concat([grp1, grp2], axis=1).dropna()
        joined.columns = ['mean_first', 'mean_second']
        joined['rel_change'] = (joined['mean_second'] - joined['mean_first']) / (joined['mean_first'].replace(0, np.nan))
        if joined['rel_change'].dropna().empty:
            print("No level shifts detected; skipping plot")
            return
        plt.figure(figsize=(6,4))
        plt.hist(joined['rel_change'].dropna(), bins=20)
        plt.title('Distribution of relative change (second half vs first half) per file')
        plt.xlabel('relative change')
        plt.tight_layout()
        plt.savefig(outpath)
        plt.close()
        print(f"Wrote level-shift plot -> {outpath}")
    except Exception as e:
        print(f"Level-shift plotting failed: {e}")

def main():
    summary_path = read_summary()
    if not summary_path:
        print("No summary_report.csv found under reports/ci or reports/; if you ran the sentinel, move summary CSV to reports/ci/ or place dq-reports.zip in repo.")
    else:
        print(f"Using summary file: {summary_path}")
        df = build_dq_findings(summary_path)
        summary = write_summary_json(df)
    # build cleaned timeseries if cleaned CSVs exist
    full = build_cleaned_timeseries()
    if full is not None:
        # make plots
        try:
            plot_missingness(full, PLOTS_DIR / 'missingness.png')
            plot_cadence(full, PLOTS_DIR / 'cadence.png')
            plot_level_shift(full, PLOTS_DIR / 'level_shifts.png')
        except Exception as e:
            print(f"Plotting error: {e}")

if __name__ == '__main__':
    main()
