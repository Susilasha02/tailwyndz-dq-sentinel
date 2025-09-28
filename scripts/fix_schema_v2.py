# scripts/fix_schema_v2.py
import pandas as pd
import sys
from pathlib import Path

EXPECTED = ["week_start","sku_id","store_id","units","price","inventory_on_hand","currency","load_ts","source_file"]

def fix_schema(infile, outdir="data/cleaned"):
    p = Path(infile)
    df = pd.read_csv(p)
    # add missing columns with defaults
    for c in EXPECTED:
        if c not in df.columns:
            if c == "currency":
                df[c] = "USD"
            elif c in ("units","price","inventory_on_hand"):
                df[c] = pd.NA
            else:
                df[c] = ""
    # reorder to expected schema + keep extra columns at end
    cols = [c for c in EXPECTED if c in df.columns] + [c for c in df.columns if c not in EXPECTED]
    df = df[cols]
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / p.name
    df.to_csv(outpath, index=False)
    print(f"Wrote schema-fixed file: {outpath} (columns now: {df.columns.tolist()})")
    return outpath

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python fix_schema_v2.py <path/to/sales_weekly_schema_v2.csv>")
        sys.exit(2)
    fix_schema(sys.argv[1])
