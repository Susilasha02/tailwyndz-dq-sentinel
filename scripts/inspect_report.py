# scripts/inspect_report.py
import json, sys, pandas as pd
from pathlib import Path

report = Path(sys.argv[1]) if len(sys.argv)>1 else Path("reports/summary_report.csv")
if report.suffix == ".csv":
    df = pd.read_csv(report)
    fails = df[df["blocking"]=="FAIL"]
    print("Failing files summary:")
    print(fails.to_string(index=False))
else:
    r = json.load(report.open())
    print(json.dumps(r, indent=2))
