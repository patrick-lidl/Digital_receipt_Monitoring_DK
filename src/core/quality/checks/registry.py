from typing import Dict, Type

from .base import BaseCheck
from .drift import AverageValueDriftCheck, RowCountDriftCheck
from .static import DistinctCountCheck, NotNullCheck, UniqueCheck

# The core registry contains generic, reusable checks.
CHECK_REGISTRY: Dict[str, Type[BaseCheck]] = {
    # Static Checks
    "not_null": NotNullCheck,
    "unique": UniqueCheck,
    "distinct_count": DistinctCountCheck,
    # Drift Checks
    "row_count_drift": RowCountDriftCheck,
    "avg_value_drift": AverageValueDriftCheck,
}
