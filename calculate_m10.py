"""KPI_09: Rueckgaben ohne Original-Beleg."""

from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from digital_receipt_monitoring.utils.metric_task import MetricTask


class CalculateM09(MetricTask):
    """KPI_09: Rueckgaben ohne Original-Beleg."""

    metric_id = 9

    @staticmethod
    def _calculate_metric(  # type: ignore[override]
        returns_without_original_receipt: SparkDataFrame,
    ) -> SparkDataFrame:
        """
        NOTE: The signature of this method intentionally differs from its
        superclass. It uses specific, named arguments to allow for automatic
        dependency discovery by the core.Job class.
        """  # noqa: D205
        return returns_without_original_receipt  # FIXME: why are there duplicates??
