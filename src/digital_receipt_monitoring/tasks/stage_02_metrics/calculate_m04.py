"""KPI_04: Bons mit Zeilenstornos bis auf 1 Artikel."""

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from digital_receipt_monitoring.utils.metric_task import MetricTask


class CalculateM04(MetricTask):
    """KPI_04: Bons mit Zeilenstornos bis auf 1 Artikel."""

    metric_id = 4

    @staticmethod
    def _calculate_metric(  # type: ignore[override]
        receipt_voids_cash_only: SparkDataFrame,
    ) -> SparkDataFrame:
        """
        NOTE: The signature of this method intentionally differs from its
        superclass. It uses specific, named arguments to allow for automatic
        dependency discovery by the core.Job class.
        """  # noqa: D205
        # ME.03 Small receipts with voids. Receipts with a Total_Gross_Value smaller or equal than
        # v_value_small_receipts that have voids are counted
        return receipt_voids_cash_only.filter(F.col("cnt_receiptlines_sales") - F.col("cnt_receiptlines_voids") == 1)
