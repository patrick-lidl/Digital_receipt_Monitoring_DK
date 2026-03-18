"""KPI_02: Rueckgaben ausserhalb der Oeffnungszeiten."""

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from digital_receipt_monitoring.utils.metric_task import MetricTask


class CalculateM02(MetricTask):
    """KPI_02: Rueckgaben ausserhalb der Oeffnungszeiten."""

    metric_id = 2

    @staticmethod
    def _calculate_metric(  # type: ignore[override]
        receipts_outside_store_hours: SparkDataFrame,
    ) -> SparkDataFrame:
        """
        NOTE: The signature of this method intentionally differs from its
        superclass. It uses specific, named arguments to allow for automatic
        dependency discovery by the core.Job class.
        """  # noqa: D205
        # Register < 80 excludes SCO
        return receipts_outside_store_hours.filter((F.col("return_fg") == 1) & (F.col("register_id") < 80))
