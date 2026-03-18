"""An end-to-end pipeline for detecting anomalies in digital receipts."""

__all__ = [
    # Stage 1
    "CashierStats",
    "ReceiptsOutsideStoreHours",
    "ReceiptVoidsCashOnly",
    # Stage 2
    "CalculateM01",
    "CalculateM02",
    "CalculateM03",
    "CalculateM04",
    "CalculateM05",
    "CalculateM06",
    "CalculateM07",
    "CalculateM08",
    "CalculateM09",
    "CalculateM10",
    # Stage 3
    "MergeReceiptAnomalies",
    "FlagCashiers",
    # Stage 4
    "ExportReceiptMonitoringJson",
    "CreateWeeklyReport",
    "JobPostProcessing",
]

# Stage 1: Data Prep
from .tasks.stage_01_data_prep.cashier_stats import CashierStats
from .tasks.stage_01_data_prep.receipt_voids_cash_only import ReceiptVoidsCashOnly
from .tasks.stage_01_data_prep.receipts_outside_store_hours import ReceiptsOutsideStoreHours

# Stage 2: Metrics
from .tasks.stage_02_metrics.calculate_m01 import CalculateM01
from .tasks.stage_02_metrics.calculate_m02 import CalculateM02
from .tasks.stage_02_metrics.calculate_m03 import CalculateM03
from .tasks.stage_02_metrics.calculate_m04 import CalculateM04
from .tasks.stage_02_metrics.calculate_m05 import CalculateM05
from .tasks.stage_02_metrics.calculate_m06 import CalculateM06
from .tasks.stage_02_metrics.calculate_m07 import CalculateM07
from .tasks.stage_02_metrics.calculate_m08 import CalculateM08
from .tasks.stage_02_metrics.calculate_m09 import CalculateM09
from .tasks.stage_02_metrics.calculate_m10 import CalculateM10

# Stage 3: Flagging
from .tasks.stage_03_flagging.flag_cashiers import FlagCashiers
from .tasks.stage_03_flagging.merge_receipt_anomalies import MergeReceiptAnomalies

# Stage 4: Output
from .tasks.stage_04_output.create_weekly_report import CreateWeeklyReport
from .tasks.stage_04_output.export_receipt_monitoring_json import ExportReceiptMonitoringJson
from .tasks.stage_04_output.job_post_processing import JobPostProcessing
