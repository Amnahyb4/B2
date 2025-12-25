from pathlib import Path
import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 180)

BASE = Path("data/processed")

def main():
    for p in BASE.glob("*.parquet"):
        print("\n" + "="*80)
        print(f"FILE: {p.name}")
        df = pd.read_parquet(p)

        print(f"SHAPE: {df.shape}")
        print("DTYPES:")
        print(df.dtypes)

        print("\nHEAD:")
        print(df.head(5).to_string(index=False))

        print("\nMISSING %:")
        print((df.isna().mean() * 100).round(2))

if __name__ == "__main__":
    main()
