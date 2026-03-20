"""KPI_08: Rueckgaben allgemein."""

from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from digital_receipt_monitoring.utils.metric_task import MetricTask


class CalculateM08(MetricTask):
    """KPI_08: Rueckgaben allgemein."""

    metric_id = 8

    def _calculate_metric(  # type: ignore[override]
        self,
        receipt_line_returns: SparkDataFrame,
    ) -> SparkDataFrame:
        """
        NOTE: The signature of this method intentionally differs from its
        superclass. It uses specific, named arguments to allow for automatic
        dependency discovery by the core.Job class.
        """  # noqa: D205
        return receipt_line_returns
