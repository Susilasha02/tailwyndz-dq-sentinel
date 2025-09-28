# tests/test_checks.py
import pandas as pd
from src import checks

def test_schema_ok():
    df = pd.DataFrame({
        "week_start":["2023-01-02"],
        "sku_id":["S001"],
        "store_id":["C001"],
        "units":[10],
        "price":[5.0],
        "inventory_on_hand":[100],
        "currency":["USD"],
        "load_ts":["2023-01-03T00:00:00Z"],
        "source_file":["erp.csv"]
    })
    df = checks.normalize_df(df)
    r = checks.check_schema(df)
    assert r["schema_ok"] is True
