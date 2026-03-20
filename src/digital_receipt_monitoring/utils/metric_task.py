"""Base class for calculating DBP metrics."""

import inspect
from abc import abstractmethod
from typing import Dict, List, Type

from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from core.task import Task, TaskMeta


# Combined metaclass
class CombinedMeta(TaskMeta):
    """Combine metaclasses."""

    pass


class MetricTask(Task, metaclass=CombinedMeta):
    """Base class for calculating DBP metrics."""

    @property
    @abstractmethod
    def metric_id(self) -> int:
        """Metric ID must be defined in the subclass."""
        pass

    def run_content(
        self,
        *args: SparkDataFrame,
        **kwargs: SparkDataFrame,
    ) -> Dict[str, SparkDataFrame]:
        """Calculate the metric and output the dataframe on receipt-level."""
        metric_df = (
            self._calculate_metric(*args, **kwargs)
            .select(
                "src_orig_store_nr",
                "receipt_dt",
                "receipt_tmsp",
                "register_id",
                "cashier_id",
                "receipt_id",
            )
            .dropDuplicates()
        )
        # Pad the integer to two digits
        return {f"dbp_kpi_{self.metric_id:02}": metric_df}

    @abstractmethod
    def _calculate_metric(self, *args: SparkDataFrame) -> SparkDataFrame:
        raise NotImplementedError

    @classmethod
    def _get_task_inputs(cls) -> Dict[str, Type]:
        """Return input parameters to run content."""
        return {
            param: value.annotation
            for param, value in inspect.signature(cls._calculate_metric).parameters.items()
            if param != "self"
        }

    @classmethod
    def _get_task_outputs(cls) -> List[str]:
        """Get output table of a task by inspecting the source code."""
        return [f"dbp_kpi_{cls.metric_id:02}"]
