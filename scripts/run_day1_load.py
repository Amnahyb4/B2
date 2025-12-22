from pathlib import Path
import sys
import pandas as pd
import logging

Root=Path(__file__).resolve().parents[1] #resolve makes it absolute

SRC=Root/"src"  
sys.path.insert(0, str(SRC))
 #Add the src/ folder to the front of Python’s import search list.(0 means highest priority)

##ETL Steps
from data_workflow.io import read_orders_csv, write_parquet, read_parquet , read_users_csv
from data_workflow.transforms import enforce_schema 
from data_workflow.config import make_paths

log=logging.getLogger(__name__) #A smarter print that knows where it came from

def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s:%(name)s:%(message)s")
    p=make_paths(Root)
    orders_raw=read_orders_csv(p.raw/"orders.csv")
    users_raw=read_users_csv(p.raw/"users.csv")

    orders=enforce_schema(orders_raw)
    
    out_orders=p.processed/"orders.parquet"
    out_users=p.processed/"users.parquet"
    write_parquet(orders, out_orders)
    write_parquet(users_raw, out_users)

    #enforce_schema fixes the data in memory; Parquet saves the fixed data to disk.

    log.info("Loaded rows: orders=%s users=%s", len(orders_raw), len(users_raw))
    log.info(f"Wrote orders to {out_orders}")
    log.info(f"Wrote users to {out_users}") 

if __name__=="__main__":
    main()