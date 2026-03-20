from typing import TYPE_CHECKING, List

from ch_utils.foundations.env_utils import get_spark

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def reindex_spark_df(input_df: "DataFrame", index_list: List[str], col_name: str) -> "DataFrame":
    """Transform Spark Dataframe to match a given index."""
    # Import pyspark only when the function is called

    return (
        get_spark()
        .createDataFrame([(i,) for i in index_list], [col_name])
        .join(input_df, on=col_name, how="left")
        .fillna(0)
    )


def col_as_list(input_df: "DataFrame", column: str, cast_to_str: bool = False, invert: bool = False) -> List:
    """Convert a pyspark column to list."""
    # Import pyspark only when the function is called
    from pyspark.sql import functions as F

    if cast_to_str:
        input_df = input_df.withColumn(column, F.col(column).cast("string"))

    output = input_df.select(column).rdd.flatMap(lambda x: x).collect()

    if invert:
        output = output[::-1]
    return output
