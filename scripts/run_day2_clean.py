from pathlib import Path
import sys
import pandas as pd
import logging

Root=Path(__file__).resolve().parents[1] #resolve makes it absolute
SRC=Root/"src"  
sys.path.insert(0, str(SRC))    

##ETL Steps
from data_workflow.io import read_parquet, write_parquet, read_orders_csv, read_users_csv
from data_workflow.transforms import enforce_schema, missingness_report, add_missing_flags, normalize_text, apply_mapping, dedupe_keep_latest, enforce_users_schema
from data_workflow.quality import require_columns, asser_non_empty, assert_unique_keys, assert_in_range
from data_workflow.config import make_paths

log=logging.getLogger(__name__)

def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

    p=make_paths(Root)

    log.info("loading raw data")
    orders_raw=read_orders_csv(p.raw/"orders.csv")
    users_raw=read_users_csv(p.raw/"users.csv")


    require_columns(orders_raw, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
    require_columns(users_raw, ["user_id", "country", "signup_date"])

    asser_non_empty(orders_raw,name="orders_raw")
    asser_non_empty(users_raw, name="users_raw")

    orders=enforce_schema(orders_raw)
    users=enforce_users_schema(users_raw)

    rep=missingness_report(orders)
    rep.to_csv(p.reports/"orders_missingness.csv")
    rep_users=missingness_report(users)
    rep_users.to_csv(p.reports/"users_missingness.csv")

    status_norm=normalize_text(orders["status"])
    mapping={
        "paid": "paid",
        "refund": "refund",
        "refunded": "refund",
    }
    orders=orders.assign(
        status_clean=apply_mapping(status_norm,mapping)
    )
    
    orders=add_missing_flags(orders, ["amount", "quantity"])

    assert_in_range(orders["amount"], lo=0, name='amount')
    assert_in_range(orders["quantity"], lo=0, name="quantity")

    write_parquet(orders, p.processed/ "orders_clean.parquet")
    write_parquet(users, p.processed / "users_clean.parquet")

    log.info("Wrote users_clean.parquet")
    log.info("wrote orders_clean.parquet")

if __name__ == "__main__":
    main()
