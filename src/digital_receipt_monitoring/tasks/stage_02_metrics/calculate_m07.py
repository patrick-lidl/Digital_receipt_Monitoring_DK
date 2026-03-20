"""KPI_07: Rueckgaben ohne Autorisierung."""

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from digital_receipt_monitoring.utils.metric_task import MetricTask


class CalculateM07(MetricTask):
    """KPI_07: Rueckgaben ohne Autorisierung."""

    metric_id = 7

    def _calculate_metric(  # type: ignore[override]
        self,
        receipt_line_returns: SparkDataFrame,
    ) -> SparkDataFrame:
        """
        NOTE: The signature of this method intentionally differs from its
        superclass. It uses specific, named arguments to allow for automatic
        dependency discovery by the core.Job class.
        """  # noqa: D205
        # add flag for returns which need no authorisation
        return receipt_line_returns.filter(
            (F.col("receipt_total_gross_val") > -self.config["v_independant_return_limit"])
            & (F.col("receipt_total_gross_val") < -self.config["v_independant_return_minimum"])
        )
