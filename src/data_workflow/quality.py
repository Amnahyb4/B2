import pandas as pd
from typing import Dict, List, Optional


#checking data quality

def require_columns(df: pd.DataFrame, cols: List[str]) -> None:
    missing_cols=[col for col in cols if col not in df.columns]
    assert not missing_cols, f"Missing required columns: {missing_cols}"

def asser_non_empty(df:pd.DataFrame, name="df") -> None:
    assert len(df) > 0, f"{name} has 0 rows"

def assert_unique_keys(df:pd.DataFrame, key:str,*, allow_na=False) -> None:
    if not allow_na:
        assert df[key].notna().all(), f"Column {key} contains NA values"
      
    dupes = df[key].duplicated(keep=False)& df[key].notna()
    assert not dupes.any(), f"Column {key} contains duplicate values: {df[dupes]}"

def assert_in_range(s: pd.Series, lo=None, hi=None, name="value") -> None:   
    x=s.dropna()
    if lo is not None:
        assert (x >= lo).all(), f"Some values in {name} are below {lo}"
    if hi is not None:
        assert (x <= hi).all(), f"Some values in {name} are above {hi}"

##lo = lower bound (minimum allowed value), 
# hi = upper bound (maximum allowed value)