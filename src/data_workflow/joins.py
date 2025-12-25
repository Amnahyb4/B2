import pandas as pd
from typing import Dict, List, Tuple

def safe_left_join(left: pd.DataFrame, right: pd.DataFrame, on: List[str], validate="many_to_one", 
                   suffixes: Tuple[str, str]= ("", "_r") ) -> pd.DataFrame: #suffixes for renaming same name columns
    return(
        left.merge(
            right,
            how="left",
            on=on,
            validate=validate,
            suffixes=suffixes,
        )
    )