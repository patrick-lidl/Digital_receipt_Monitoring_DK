import logging
from typing import Dict

import pandas as pd
from pandas.core.api import DataFrame

from ch_utils.data.general import add_static_columns
from core.data_manager import TableNotFoundError
from core.task import Task
from digital_receipt_monitoring.utils.kpi_monitoring_data import KpiMonitoringData
from digital_receipt_monitoring.utils.report_generator import ReportGenerator


class CreateWeeklyReport(Task):
    """
    Create an HTML report using arakawa.

    There should be the following classes:
        1. DataClass: processes and saves the data, has loading functions for the stats
        2. PlottingClass: visualizes the data.
        3. ReportGenerator: uses the data + plotting classes to generate the weekly report

    """

    def run_content(
        self,
        flagged_cashier_kpis: DataFrame,
        receipt_monitoring_details: DataFrame,
        vt_structure: DataFrame,
    ) -> Dict[str, DataFrame]:
        """Create an HTML report using arakawa."""
        kpi_monitoring_data = self._load_full_data(flagged_cashier_kpis, receipt_monitoring_details, vt_structure)
        logging.info(f"Weeks found in data by KpiMonitoringData: {kpi_monitoring_data.weeks_in_data}")

        if len(kpi_monitoring_data.weeks_in_data) < 2:
            logging.error("Report generation not implemented, if no historical data exists.")
            return {}

        last_year_cw = kpi_monitoring_data.weeks_in_data[-1].replace("-", "-KW")
        report_name = f"{last_year_cw}_dbp_report.html"

        report_generator = ReportGenerator(
            kpi_monitoring_data, title="Digital Receipt Monitoring", sub_title=last_year_cw
        )

        report_generator.add_overview_stats()
        check_log = self.data_manager.load_check_results(self._run_state.run_id).toPandas()
        report_generator.add_check_log(check_log)
        report_generator.add_flag_sections()
        report_generator.add_anomaly_section()

        logging.info(f"Saving HTML report '{report_name}' via DataManager...")
        self.data_manager.save_file(
            writer_func=report_generator.save,
            file_name=report_name,
            project_name="digital_receipt_monitoring",
        )

        return {}

    def _load_full_data(
        self,
        flagged_cashier_kpis: DataFrame,
        receipt_monitoring_details: DataFrame,
        vt_structure: DataFrame,
    ) -> KpiMonitoringData:
        logging.debug(f"Initial flagged_cashier_kpis shape: {flagged_cashier_kpis.shape}")
        logging.debug(f"Initial receipt_monitoring_details shape: {receipt_monitoring_details.shape}")
        logging.debug(f"Initial vt_structure shape: {vt_structure.shape}")

        if flagged_cashier_kpis.empty:
            logging.warning("Initial 'flagged_cashier_kpis' DataFrame is empty")
        if receipt_monitoring_details.empty:
            logging.warning("Initial 'receipt_monitoring_details' DataFrame is empty")

        columns_to_add = {
            "run_id": str(self._run_state.run_id),
            "run_timestamp": str(self._run_state.run_timestamp),
        }

        flagged_cashier_kpis = self._combine_with_historical_data(
            flagged_cashier_kpis, columns_to_add, "flagged_cashier_kpis"
        )
        receipt_monitoring_details = self._combine_with_historical_data(
            receipt_monitoring_details, columns_to_add, "receipt_monitoring_details"
        )
        vt_structure = self._combine_with_historical_data(vt_structure, columns_to_add, "vt_structure")

        logging.debug(f"Combined flagged_cashier_kpis shape: {flagged_cashier_kpis.shape}")
        logging.debug(f"Combined receipt_monitoring_details shape: {receipt_monitoring_details.shape}")
        logging.debug(f"Combined vt_structure shape: {vt_structure.shape}")

        return KpiMonitoringData(
            flagged_cashier_kpis,
            receipt_monitoring_details,
            vt_structure,
            look_back_weeks=int(self.config.report.look_back_weeks),
        )

    def _combine_with_historical_data(
        self, df: DataFrame, columns_to_add: Dict[str, str], table_name: str
    ) -> DataFrame:
        """Load historical data and append working data with extra columns."""
        df_augm = add_static_columns(df, columns_to_add)
        df_augm = self._standardize_columns(df_augm)
        try:
            df_hist = self.data_manager.load_table(table_name, DataFrame, "historical")
            df_hist = self._standardize_columns(df_hist)
        except TableNotFoundError:
            logging.warning(f"No historical data has been logged for {table_name} yet")
            return df_augm

        return pd.concat([df_hist, df_augm])

    @staticmethod
    def _standardize_columns(df: DataFrame) -> DataFrame:
        """
        Standardize DataFrame columns.

        Converts all columns to uppercase, except for specific columns
        that should remain lowercase.
        """
        # List of columns to keep in lowercase
        lowercase_exceptions = ["run_id", "run_timestamp"]

        new_columns = []
        for col in df.columns:
            # Check the lowercase version of the column name
            if col.lower() in lowercase_exceptions:
                new_columns.append(col.lower())  # Ensure it is lowercase
            else:
                new_columns.append(col.upper())  # Convert all others to uppercase

        df.columns = new_columns
        return df
