"""Load receipt voids cash only."""

import logging
from typing import Dict, Union

from pandas.core.api import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from core.task import Task


class ReceiptVoidsCashOnly(Task):
    """Load receipt voids cash only."""

    def run_content(
        self,
        receipt_line_std_til: SparkDataFrame,
        receipt_header: SparkDataFrame,
        exclusively_cash_payments: SparkDataFrame,
    ) -> Dict[str, Union[DataFrame, SparkDataFrame]]:
        """Load all data from snowflake, to be subsequently read from UC."""
        return {
            "receipt_voids_cash_only": self._create_receipt_voids_cash_only(
                receipt_line_std_til, receipt_header, exclusively_cash_payments
            ),
        }

    def _create_receipt_voids_cash_only(
        self,
        receipt_line_std_til: SparkDataFrame,
        receipt_header: SparkDataFrame,
        exclusively_cash_payments: SparkDataFrame,
    ) -> SparkDataFrame:
        # Prepare receipt header
        receipt_header_cash_sales = (
            receipt_header
            # Exclude SCO
            .filter(F.col("register_id") < 80)
            # Use only sales transactions
            .filter(F.col("transaction_type_cd") == 100)
            .select(
                "src_orig_store_nr",
                "receipt_dt",
                "receipt_tmsp",
                "register_id",
                "cashier_id",
                "receipt_id",
                "transaction_id",
                "cnt_receiptlines_sales",
                "cnt_receiptlines_voids",
            )
        )
        # Use only cash transactions
        if self.config.filter_to_cash_only_transactions:
            logging.info("Filtering receipt voids to cash only transactions")
            receipt_header_cash_sales = receipt_header_cash_sales.join(
                exclusively_cash_payments.select("transaction_id"),
                on="transaction_id",
                how="inner",
            )
        return (
            receipt_line_std_til
            # Use only lines with voids
            .filter(F.col("void_fg") == 1)
            # Add necessary columns + filter using receipt header
            .join(receipt_header_cash_sales, on="transaction_id", how="inner")
            .select(
                "src_orig_store_nr",
                "receipt_dt",
                "receipt_tmsp",
                "register_id",
                "cashier_id",
                "receipt_id",
                "cnt_receiptlines_sales",
                "cnt_receiptlines_voids",
            )
            .dropDuplicates()
        )
