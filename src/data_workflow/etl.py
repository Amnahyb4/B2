import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd 

from data_workflow.io import read_orders_csv, read_users_csv, write_parquet
from data_workflow.joins import safe_left_join
from data_workflow.quality import asser_non_empty,assert_unique_keys,require_columns
from data_workflow.transforms import add_missing_flags, add_outlier_flag, add_time_parts, apply_mapping,enforce_schema,normalize_text,parse_datetime,winsorize

log=logging.getLogger(__name__)

##ETLConfig is needed to centralize configuration, 
# avoid hard-coding, improve reusability, and make the ETL pipeline safe, clean, and maintainable.

@dataclass(frozen=True)
class ETLConfig:
    """configuration for ETL pipline run"""
    root: Path
    raw_orders:Path
    raw_users:Path
    out_orders_clean:Path
    out_users:Path
    out_analytics:Path
    run_meta:Path

def load_data(cfg:ETLConfig)-> tuple[pd.DataFrame, pd.DataFrame]:
    """Extract raw data"""
    orders=read_orders_csv(cfg.raw_orders)
    users=read_users_csv(cfg.raw_users)
    return orders, users

def transform(orders_raw:pd.DataFrame, users: pd.DataFrame)-> pd.DataFrame:
    """transform clean enrich validate data"""
    require_columns(orders_raw,["order_id", "user_id", "amount", "quantity", "created_at", "status"])
    require_columns(users, ["user_id", "country", "signup_date"])

    asser_non_empty(orders_raw, "orders_raw")
    asser_non_empty(users,"users")

    assert_unique_keys(users, "user_id")

    orders=enforce_schema(orders_raw)

    status_norm=normalize_text(orders["status"])
    mapping={"paid":"paid", "refund":"refund", "refunded":"refunded"}
    orders=orders.assign(status_clean=apply_mapping(status_norm,mapping))

    orders=add_missing_flags(orders, cols=["amount", "quantity"])

    orders=parse_datetime(orders, col=["created_at"], utc=True)
    orders=add_time_parts(orders, ts_col="created_at")

    joined=safe_left_join(orders, users, on="user_id", validate="many_to_one")
    assert len(joined)==len(orders), "join explosion happened"

    joined==joined.assign(amount_winsor=winsorize(joined["amount"]))
    joined=add_outlier_flag(joined, "amount", k=1.5)

    return joined

def load_output(analytics: pd.DataFrame, useres: pd.DataFrame, cfg:ETLConfig):
    """LOAD Output in disk"""

    write_parquet(useres, cfg.out_users)
    write_parquet(analytics, cfg.out_analytics)

def write_run_meta(cfg: ETLConfig, *, analytics: pd.DataFrame) -> None:
    """Write run metadata for reproducibility."""
    missing_created_at = int(analytics["created_at"].isna().sum())
    country_match_rate = 1.0 - float(analytics["country"].isna().mean())
    
    meta = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "rows_out": int(len(analytics)),
        "missing_created_at": missing_created_at,
        "country_match_rate": country_match_rate,
        "config": {k: str(v) for k, v in asdict(cfg).items()},
    }
    
    cfg.run_meta.parent.mkdir(parents=True, exist_ok=True)
    cfg.run_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")

def run_etl(cfg: ETLConfig) -> None:
    """Run the complete ETL pipeline."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    
    log.info("Loading inputs")
    orders_raw, users = load_data(cfg)
    
    log.info("Transforming (orders=%s, users=%s)", len(orders_raw), len(users))
    analytics = transform(orders_raw, users)
    
    log.info("Writing outputs to %s", cfg.out_analytics.parent)
    load_output(analytics, users, cfg)
    
    log.info("Writing run metadata: %s", cfg.run_meta)
    write_run_meta(cfg, analytics=analytics)
    
    log.info("ETL complete: %s rows in analytics table", len(analytics))


