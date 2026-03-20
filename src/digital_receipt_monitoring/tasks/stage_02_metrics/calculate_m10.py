"""KPI_10: Bonabbrueche."""

import logging

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from digital_receipt_monitoring.utils.metric_task import MetricTask


class CalculateM10(MetricTask):
    """KPI_10: Bonabbrueche."""

    metric_id = 10

    def _calculate_metric(  # type: ignore[override]
        self,
        receipt_header: SparkDataFrame,
        exclusively_cash_payments: SparkDataFrame,
    ) -> SparkDataFrame:
        """
        NOTE: The signature of this method intentionally differs from its
        superclass. It uses specific, named arguments to allow for automatic
        dependency discovery by the core.Job class.
        """  # noqa: D205
        # ME.11 Receipt Cancellation: value(a) and number (b) of receipts which were cancelled differentiated if they
        # are sales receipts, return receipts or receipt to validate POS-Update
        output = (
            receipt_header
            # use only regular POS: REGISTER_ID 01 - 59
            .filter(F.col("register_id") < 80)
            # use only sales cancelations
            .filter(F.col("transaction_type_cd") == 200)
        )
        # Use only cash transactions
        if self.config.filter_to_cash_only_transactions:
            logging.info(f"Filtering to cash only transactions for KPI {self.metric_id}")
            output = output.join(
                exclusively_cash_payments.select("transaction_id"),
                on="transaction_id",
                how="inner",
            )
        return output
