"""Calculate Cashier Stats."""

from typing import Dict, Union

from pandas.core.api import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from core.task import Task


class CashierStats(Task):
    """Calculate Cashier Stats."""

    def run_content(
        self,
        receipt_header: SparkDataFrame,
    ) -> Dict[str, Union[DataFrame, SparkDataFrame]]:
        """Load all data from snowflake, to be subsequently read from UC."""
        return {
            "cashier_stats": self._get_cashier_stats(receipt_header),
        }

    @staticmethod
    def _get_cashier_stats(receipt_header: SparkDataFrame) -> SparkDataFrame:
        """Prepare the spark df for the normalisation of cashier ids."""
        return (
            receipt_header
            # only turnover relevant sales or returns
            .filter(F.col("transaction_type_cd").isin([100, 150]))
            .withColumn(
                "year_week",
                F.concat_ws(
                    "-",
                    F.expr("EXTRACT(YEAROFWEEK FROM receipt_dt)"),
                    F.format_string("%02d", F.expr("WEEKOFYEAR(receipt_dt)")),
                ),
            )
            # aggregate to cashier / week level
            .groupby(
                "src_orig_store_nr",
                "cashier_id",
                "year_week",
            )
            .agg(
                F.count("transaction_id").alias("total_receipts"),
            )
            .select(
                "src_orig_store_nr",
                "cashier_id",
                "year_week",
                "total_receipts",
            )
        )
