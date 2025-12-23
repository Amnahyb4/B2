import pandas as pd 

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("float"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"), 
        ## errors="coerce" will convert invalid parsing to NaN
    )
def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "num_missing": df.isna().sum(),
        "pct_missing": df.isna().mean()*100
    }).sort_values(by="pct_missing", ascending=False)

def add_missing_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for col in cols:
        flag_col = f"{col}_missing"
        df[flag_col] = df[col].isna()
    return df
