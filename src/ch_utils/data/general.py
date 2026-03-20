from __future__ import annotations

from typing import Any, Dict, Union

import pandas as pd
from pandas import DataFrame as PandasDataFrame
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as SparkDataFrame


def add_static_columns(
    data: Union[PandasDataFrame, SparkDataFrame],
    columns_to_add: Dict[str, Any],
) -> Union[PandasDataFrame, SparkDataFrame]:
    """
    Add static columns to a Dataframe.

    Adds multiple columns with static values to a DataFrame, handling
    both pandas and Spark types.
    """
    if isinstance(data, PandasDataFrame):
        # Make a copy to avoid modifying the original DataFrame in place
        data_augm = data.copy()
        for col_name, value in columns_to_add.items():
            # Special handling for pandas timestamp conversion
            if "timestamp" in col_name:
                data_augm[col_name] = pd.to_datetime(value)
            else:
                data_augm[col_name] = value
        return data_augm

    elif isinstance(data, SparkDataFrame):
        data_augm = data
        for col_name, value in columns_to_add.items():
            data_augm = data_augm.withColumn(col_name, F.lit(value))
        return data_augm

    else:
        raise TypeError(f"Unsupported data type for adding columns: {type(data)}")
