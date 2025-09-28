# scripts/fix_tz_and_dedupe.py
import pandas as pd
import sys
from pathlib import Path

def fix_file(infile, outdir="data/cleaned"):
    p = Path(infile)
    df = pd.read_csv(p)
    # preserve original
    df_orig = df.copy()
    # parse load_ts with utc where possible (handles offsets like +05:30 or Z)
    df['load_ts_parsed'] = pd.to_datetime(df['load_ts'], errors='coerce', utc=True)
    # For entries where parse produced NaT, try parse without utc (naive) and then treat as local UTC (best-effort)
    mask = df['load_ts_parsed'].isna()
    if mask.any():
        try:
            df.loc[mask, 'load_ts_parsed'] = pd.to_datetime(df.loc[mask, 'load_ts'], errors='coerce')
            # local (naive) timestamps -> assume they are UTC (if that's wrong, you'll need domain input)
            # convert to UTC-aware
            df.loc[mask, 'load_ts_parsed'] = pd.to_datetime(df.loc[mask, 'load_ts_parsed']).dt.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
        except Exception:
            pass
    # create output dir
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    # If PK columns exist, dedupe
    pk = ['week_start','sku_id','store_id']
    if all(c in df.columns for c in pk):
        # ensure week_start sorted as datetime
        df['week_start'] = pd.to_datetime(df['week_start'], errors='coerce')
        # sort so the latest load_ts_parsed is last
        df_sorted = df.sort_values(pk + ['load_ts_parsed'])
        df_clean = df_sorted.drop_duplicates(subset=pk, keep='last').copy()
    else:
        # fallback: no pk - just keep as-is
        df_clean = df.copy()
    # remove helper column before writing (but keep load_ts as-is)
    df_clean = df_clean.drop(columns=['load_ts_parsed'], errors='ignore')
    outpath = outdir / p.name
    df_clean.to_csv(outpath, index=False)
    print(f"Wrote cleaned file: {outpath} (original rows: {len(df)}, cleaned rows: {len(df_clean)})")
    return outpath

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python fix_tz_and_dedupe.py <path/to/file.csv> [outdir]")
        sys.exit(2)
    infile = sys.argv[1]
    outdir = sys.argv[2] if len(sys.argv) > 2 else "data/cleaned"
    fix_file(infile, outdir)
