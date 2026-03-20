"""KPI_06: Bons mit Zeilenstorno des letzten Artikels."""

import logging

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.window import Window

from digital_receipt_monitoring.utils.metric_task import MetricTask


class CalculateM06(MetricTask):
    """KPI_06: Bons mit Zeilenstorno des letzten Artikels."""

    metric_id = 6

    def _calculate_metric(  # type: ignore[override]
        self,
        receipt_line_std_til: SparkDataFrame,
        receipt_header: SparkDataFrame,
        exclusively_cash_payments: SparkDataFrame,
    ) -> SparkDataFrame:
        """
        NOTE: The signature of this method intentionally differs from its
        superclass. It uses specific, named arguments to allow for automatic
        dependency discovery by the core.Job class.
        """  # noqa: D205
        # ME.04 Voids of the last article without appearing in the next: receipts (a) and value (b) of voids of the last
        # article in general and distinugishing between the one in which the article appears in the next receipts and
        # the ones in which it does not appear.

        receipt_successors = (
            receipt_header
            # exlude SCO & unrelevant
            .filter(F.col("register_id") < 80)
            .filter(F.col("turnover_relevant_fg") == 1)
            # add transaction id of the next receipt
            .withColumn(
                "transaction_id_successor",
                F.lag("transaction_id", -1).over(
                    Window.partitionBy("receipt_dt", "src_orig_store_nr", "register_id").orderBy(
                        F.col("receipt_tmsp").asc()
                    )
                ),
            )
            .select(
                "transaction_id",
                "transaction_id_successor",
                "src_orig_store_nr",
                "receipt_dt",
                "receipt_tmsp",
                "register_id",
                "cashier_id",
                "receipt_id",
            )
        )

        receipt_successors_articles = (
            receipt_line_std_til
            # Filter revelant lines (101, 102 are standard sales)
            .filter(F.col("transaction_type_cd").isin([101, 102]))
            .withColumnRenamed("transaction_id", "transaction_id_successor")
            .select("transaction_id_successor", "item_sid", "ean_barcode")
            .dropDuplicates()
        )

        # Use only cash transactions
        if self.config.filter_to_cash_only_transactions:
            logging.info(f"Filtering to cash only transactions for KPI {self.metric_id}")
            receipt_line_std_til = receipt_line_std_til.join(
                exclusively_cash_payments.select("transaction_id"),
                on="transaction_id",
                how="inner",
            )
        return (
            receipt_line_std_til
            # Filter revelent lines (101, 102 are standard sales, 170 is voids with void_fg)
            .filter(F.col("transaction_type_cd").isin([101, 102, 170, 172]))
            # filter last article of each receipt
            .withColumn(
                "last_article",
                F.first("receiptline_nr").over(
                    Window.partitionBy("transaction_id").orderBy(F.col("receiptline_nr").desc())
                ),
            )
            .filter(F.col("receiptline_nr") == F.col("last_article"))
            # filter last article that have been cancelled
            .filter(F.col("void_fg") == 1)
            # add & filter by transaction_id of next receipt at register
            .join(receipt_successors, on="transaction_id", how="inner")
            # filter by items of next receipt
            .join(
                receipt_successors_articles,
                on=["transaction_id_successor", "item_sid", "ean_barcode"],
                how="inner",
            )
        )
