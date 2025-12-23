import pandas as pd
from typing import Dict, List, Optional

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("float"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"), 
        ## errors="coerce" will convert invalid parsing to NaN
    )

def enforce_users_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        user_id=df["user_id"].astype("string"),
        country=df["country"].astype("string"),
        signup_date=pd.to_datetime(df["signup_date"], errors="coerce"),
    )


def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "num_missing": df.isna().sum(),
        "pct_missing": df.isna().mean()*100
    }).sort_values(by="pct_missing", ascending=False)

def add_missing_flags(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for col in cols:
        flag_col = f"{col}_missing"
        df[flag_col] = df[col].isna()
    return df

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

