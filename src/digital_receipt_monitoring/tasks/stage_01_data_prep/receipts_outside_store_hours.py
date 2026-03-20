"""Filter to only receipts that are outside of store opening hours."""

from typing import Dict

from pandas.core.api import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from core.task import Task


class ReceiptsOutsideStoreHours(Task):
    """Filter to only receipts that are outside of store opening hours."""

    def run_content(
        self,
        receipt_header: SparkDataFrame,
        store_opening_hours_hist: SparkDataFrame,
    ) -> Dict[str, DataFrame]:
        """
        Filter to only receipts that are outside of store opening hours.

        Opening hour and closing hour are also calculated with (w) a tolerance to be able to calculate
        kpis considering a tolerance before opening and after closing a store.

        NOTE: receipt_header only has transaction_type_cd 100, 150, 200, ... and only one entry per transaction_id

        """
        opening_hours = self._prep_snowflake_opening_hours(store_opening_hours_hist)

        # join opening hours to receipt header
        receipts_with_opening_hours = (
            receipt_header
            # Only sales and returns are relevant
            .filter(F.col("transaction_type_cd").isin(100, 150)).join(
                opening_hours,
                on=["src_orig_store_nr", "receipt_dt"],
                how="inner",
            )
        )

        receipts_outside_store_hours = self._get_receipts_outside_store_hours(receipts_with_opening_hours)

        return {"receipts_outside_store_hours": receipts_outside_store_hours}

    def _prep_snowflake_opening_hours(self, df: SparkDataFrame) -> SparkDataFrame:
        return (
            df.withColumnRenamed("dat_formdate", "receipt_dt")
            # Safely create timestamps only on open days; leave as null on closed days
            .withColumn(
                "opening_timestamp",
                F.when(
                    F.col("oeffnungstag_fg") == 1,
                    F.to_timestamp(
                        F.concat_ws(" ", F.col("receipt_dt"), F.col("opening_timestamp")), "yyyy-MM-dd HH:mm:ss"
                    ),
                ),
            )
            .withColumn(
                "closing_timestamp",
                F.when(
                    F.col("oeffnungstag_fg") == 1,
                    F.to_timestamp(
                        F.concat_ws(" ", F.col("receipt_dt"), F.col("closing_timestamp")), "yyyy-MM-dd HH:mm:ss"
                    ),
                ),
            )
            # Keep oeffnungstag_fg for downstream logic
            .select("src_orig_store_nr", "receipt_dt", "oeffnungstag_fg", "opening_timestamp", "closing_timestamp")
        )

    def _get_receipts_outside_store_hours(self, receipts_with_opening_hours: SparkDataFrame) -> SparkDataFrame:
        return (
            receipts_with_opening_hours
            # KEEP receipts if they are outside opening hours OR if the store is closed entirely
            .filter(
                (F.col("oeffnungstag_fg") == 0)
                | ~(F.col("receipt_tmsp").between(F.col("opening_timestamp"), F.col("closing_timestamp")))
            )
            # FLAG receipts outside the tolerance window (closed days are automatically flagged as 1)
            .withColumn(
                "outside_tolerance_fg",
                F.when(
                    F.col("oeffnungstag_fg") == 0,
                    1,  # Definitely outside tolerance if the store is closed
                ).otherwise(
                    1
                    - F.col("receipt_tmsp")
                    .between(
                        F.col("opening_timestamp")
                        - F.expr(f"INTERVAL {self.config['v_opening_hours_tolerance_opening']} minutes"),
                        F.col("closing_timestamp")
                        + F.expr(f"INTERVAL {self.config['v_opening_hours_tolerance_closing']} minutes"),
                    )
                    .cast("int")
                ),
            )
            # add return flag
            .withColumn("return_fg", (F.col("transaction_type_cd") == 150).cast("int"))
            # select used columns
            .select(
                "src_orig_store_nr",
                "receipt_dt",
                "receipt_tmsp",
                "register_id",
                "cashier_id",
                "receipt_id",
                "outside_tolerance_fg",
                "return_fg",
            )
        )
