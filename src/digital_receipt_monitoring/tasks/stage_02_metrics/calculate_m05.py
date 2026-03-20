"""KPI_05: Bons mit Zeilenstornos nach der letzten Zwischensumme."""

import logging

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.window import Window

from digital_receipt_monitoring.utils.metric_task import MetricTask


class CalculateM05(MetricTask):
    """KPI_05: Bons mit Zeilenstornos nach der letzten Zwischensumme."""

    metric_id = 5

    def _identify_last_n_subtotal(
        self,
        subtotal_line_std_til: SparkDataFrame,
        exclusively_cash_payments: SparkDataFrame,
    ) -> SparkDataFrame:
        # Use only cash transactions
        if self.config.filter_to_cash_only_transactions:
            logging.info(f"Filtering to cash only transactions for KPI {self.metric_id}")
            subtotal_line_std_til = subtotal_line_std_til.join(
                exclusively_cash_payments.select("transaction_id"),
                on="transaction_id",
                how="inner",
            )
        return (
            subtotal_line_std_til
            # the last article has the highest row number so ordering descending it will be at the first position
            .withColumn(
                "n_subtotal",
                F.row_number().over(Window.partitionBy("transaction_id").orderBy(F.col("receiptline_nr").desc())),
            )
            # filter n last subtotal of each receipt.
            .filter(F.col("n_subtotal") == self.config["v_n_last_subtotal"])
            .withColumnRenamed("receiptline_nr", "receiptline_nr_last_subttl")
            .select(
                "src_orig_store_nr",
                "receipt_dt",
                "receipt_tmsp",
                "register_id",
                "cashier_id",
                "receipt_id",
                "transaction_id",
                "receiptline_nr_last_subttl",
            )
        )

    def _calculate_metric(  # type: ignore[override]
        self,
        receipt_line_std_til: SparkDataFrame,
        subtotal_line_std_til: SparkDataFrame,
        exclusively_cash_payments: SparkDataFrame,
    ) -> SparkDataFrame:
        """
        NOTE: The signature of this method intentionally differs from its
        superclass. It uses specific, named arguments to allow for automatic
        dependency discovery by the core.Job class.
        """  # noqa: D205
        # ME.05 Article voids after subtotals at the end of the receipt: number (a) and value(b) of voided articles
        # after cashier uses subtotal at the end of a receipt to calculate this KPI we have to identify the last
        # subtotal of a receipt and validate the events afterwards. In case there were only cancellations
        # (171,172,173) the receipt is counted and the value of the cancellation is sumed up

        # identify the last n subtotal of the receipts: v_n_last_subtotal
        last_subtotal_receipts = self._identify_last_n_subtotal(subtotal_line_std_til, exclusively_cash_payments)

        # FIXME: should receipt_line_std_til be filtered? remove returns?
        return (
            receipt_line_std_til
            # .filter(F.col("return_fg") == 0)
            # Filter only lines after last n subtotal
            .join(last_subtotal_receipts, on="transaction_id", how="inner")
            .filter(F.col("receiptline_nr") > F.col("receiptline_nr_last_subttl"))
            # keep only receipts which have only cancellations after subtotal
            .withColumn(
                "is_cancellation",
                F.when(F.col("transaction_type_cd").isin(["170", "171", "172", "173"]), 1).otherwise(0),
            )
            .withColumn(
                "total_transactions",
                F.count("transaction_id").over(Window.partitionBy("transaction_id")),
            )
            .withColumn(
                "total_cancellations",
                F.sum("is_cancellation").over(Window.partitionBy("transaction_id")),
            )
            .filter(F.col("total_transactions") == F.col("total_cancellations"))
            # refilter transaction_type_cd
            # NOTE: remove duplicates since 170 & 172 are original positions of 171, 173
            .filter(F.col("transaction_type_cd").isin("171", "173"))
            # count how many different articles where voided
            .withColumn(
                "count_voided_articles",
                F.approx_count_distinct("ean_barcode").over(  # requires pyspark >= 2.1
                    Window.partitionBy("transaction_id")
                ),
            )
            # keep only receipts that have more or equal different items voided
            # than defined in config v_distinct_articles_voids_subtotal
            .filter(F.col("count_voided_articles") >= self.config["v_distinct_articles_voids_subtotal"])
        )
