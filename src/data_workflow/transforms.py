import pandas as pd
from typing import Dict, List, Optional

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        order_id=df["order_id"].astype("string").str.strip(),
        user_id=df["user_id"].astype("string").str.strip().str.zfill(4),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("float"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"), 
        ## errors="coerce" will convert invalid parsing to NaN
    )

def enforce_users_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        user_id=df["user_id"].astype("string").str.strip().str.zfill(4),
        country=df["country"].astype("string").str.strip(),
        signup_date=pd.to_datetime(df["signup_date"], errors="coerce"),
    )


def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "num_missing": df.isna().sum(),
        "pct_missing": df.isna().mean()*100
    }).sort_values(by="pct_missing", ascending=False)

def add_missing_flags(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    out=df.copy()
    for col in cols:
        out[f"{col}__isna"]=out[col].isna()
    return out

def normalize_text(s: pd.Series) -> pd.Series:
    return s.str.strip().str.lower().str.replace(r"\s+", " ", regex=True)

def apply_mapping(s: pd.Series, mapping: Dict[str, str]) -> pd.Series:
    return s.map(mapping).fillna(s) #fillna Because map() turns unmatched values into NaN.

def dedupe_keep_latest(df: pd.DataFrame, key_cols: List[str], ts_cols: str) -> pd.DataFrame:
    return(
        df.sort_values(ts_cols) #oldest to newest
        .drop_duplicates(subset=key_cols, keep="last") #duplicates are defined by keys not the whole row
        .reset_index(drop=True)
    )

def parse_datetime(df: pd.DataFrame, col: str, *, utc: bool = True) -> pd.DataFrame:
    return df.assign(
        **{col: pd.to_datetime(df[col], errors="coerce", utc=utc)}
    )
def add_time_parts(df: pd.DataFrame, col:str) -> pd.DataFrame:
    dt = df[col]
    return df.assign(
        date=dt.dt.date,
        year=dt.dt.year,
        month=dt.dt.to_period("M").astype("string"),
        day=dt.dt.day_name(),
        hour=dt.dt.hour,
       
    )

def iqr_bounds(s:pd.Series, k:float=1.5) -> tuple:
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - k * iqr
    upper_bound = q3 + k * iqr
    return lower_bound, upper_bound

def winsorize(s:pd.Series, lo:float=0.01, hi:float=0.99)-> pd.Series:
    a,b=s.quantile(lo), s.quantile(hi)
    return s.clip(lower=a, upper=b)
    

def add_outlier_flag(df: pd.DataFrame, col:str, *, k:float=1.5)-> pd.DataFrame:
    lo, hi = iqr_bounds(df[col], k=k)
    flag_col = f"{col}_outlier"
    return df.assign(
        **{flag_col: (df[col] < lo) | (df[col] > hi)}
    )

