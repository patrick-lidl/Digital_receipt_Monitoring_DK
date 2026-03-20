"""KPI_01: Bons ausserhalb der Oeffnungszeiten inkl. Toleranz."""

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from digital_receipt_monitoring.utils.metric_task import MetricTask


class CalculateM01(MetricTask):
    """KPI_01: Bons ausserhalb der Oeffnungszeiten inkl. Toleranz."""

    metric_id = 1

    @staticmethod
    def _calculate_metric(  # type: ignore[override]
        receipts_outside_store_hours: SparkDataFrame,
    ) -> SparkDataFrame:
        """
        NOTE: The signature of this method intentionally differs from its
        superclass. It uses specific, named arguments to allow for automatic
        dependency discovery by the core.Job class.
        """  # noqa: D205
        return receipts_outside_store_hours.filter((F.col("outside_tolerance_fg") == 1) & (F.col("return_fg") == 0))
