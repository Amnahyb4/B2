import pandas as pd 

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("float"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"), 
        ## errors="coerce" will convert invalid parsing to NaN
    )