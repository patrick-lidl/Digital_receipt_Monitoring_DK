"""KPI_03: Anzahl Zeilenstornos."""

from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from digital_receipt_monitoring.utils.metric_task import MetricTask


class CalculateM03(MetricTask):
    """KPI_03: Anzahl Zeilenstornos."""

    metric_id = 3

    @staticmethod
    def _calculate_metric(  # type: ignore[override]
        receipt_voids_cash_only: SparkDataFrame,
    ) -> SparkDataFrame:
        """
        NOTE: The signature of this method intentionally differs from its
        superclass. It uses specific, named arguments to allow for automatic
        dependency discovery by the core.Job class.
        """  # noqa: D205
        # ME.02 Voids: Total number of voided lines (a) and value of voided lines (b)
        return receipt_voids_cash_only
