import pandas as pd
from typing import Dict, List, Tuple
from pathlib import Path
import sys
import logging

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
log = logging.getLogger(__name__)   

Root=Path(__file__).resolve().parents[1] 
SRC=Root/"src"
sys.path.insert(0, str(SRC))

from data_workflow.io import read_parquet, write_parquet
from data_workflow.joins import safe_left_join  
from data_workflow.config import make_paths
from data_workflow.quality import require_columns, asser_non_empty, assert_unique_keys, assert_in_range
from data_workflow.transforms import parse_datetime, add_time_parts, iqr_bounds, winsorize, add_outlier_flag


def main():
    p=make_paths(Root)

    orders_clean=read_parquet(p.processed/"orders_clean.parquet")
    users_clean=read_parquet(p.processed/"users.parquet")

    require_columns(orders_clean, ["order_id", "user_id", "amount", "quantity", "created_at", "status_clean"])
    require_columns(users_clean, ["user_id", "country", "signup_date"])

    asser_non_empty(orders_clean, name="orders_clean")
    asser_non_empty(users_clean, name="users_clean")

    assert_unique_keys(orders_clean, "order_id", allow_na=False)

    assert_in_range(orders_clean["amount"], lo=0, hi=None, name="amount")
    assert_in_range(orders_clean["quantity"], lo=1, hi=None, name="quantity")

    #time
    orders_parse=parse_datetime(orders_clean, "created_at", utc=True)
    log.info(f"created_at dtype: {orders_parse['created_at'].dtype}")
    log.info( f"missing created_at: {orders_parse['created_at'].isna().sum()}")
   


    orders_time_parts = add_time_parts(orders_parse, "created_at")

    join_orders = safe_left_join(
        orders_time_parts,
        users_clean,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_r")
    )
    if len(join_orders)!=len(orders_time_parts):
        raise AssertionError ("join explosion happened")

    # validation after join
    log.info(f"joined shape: {join_orders.shape}")
    log.info(f"joined head:\n{join_orders.head().to_string()}")
    log.info(f"joined tail:\n{join_orders.tail().to_string()}")

    log.info(
    f"order_id uniqueness check (top counts):\n"
    f"{join_orders['order_id'].value_counts().head()}")
    

    # outlier treatment
    join_orders=join_orders.assign(winsor=winsorize(join_orders["amount"])) 
    join_orders=add_outlier_flag(join_orders,"amount", k=1.5)

    

    out_path=p.processed/ "analytics_table.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    join_orders.to_parquet(out_path, index=False)
    log.info("wrote: %s", out_path)

if __name__ == "__main__":
    main()




