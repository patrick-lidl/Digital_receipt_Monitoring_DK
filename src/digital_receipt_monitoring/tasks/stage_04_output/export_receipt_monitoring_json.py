import logging
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from pandas.core.api import DataFrame
from pandas.core.groupby import DataFrameGroupBy

from ch_utils.connectors.my_api_client import MyApiClient
from core.task import Task


class OutputConverter:
    """
    Converts cashier and receipt dataframes into a structured JSON-like dictionary.

    This class takes two pandas DataFrames—one with flagged Key Performance Indicators (KPIs)
    for cashiers and another with detailed receipt information—and processes them into a
    nested list of dictionaries, structured by week, store, and KPI.
    """

    def __init__(self, config: Any) -> None:
        """
        Initializes the converter with a configuration object.

        Args:
            config (Any): An object expected to have attributes like `column_mapping`
                          used during the conversion process.
        """
        self.config = config
        self._grouped_receipts: Optional[DataFrameGroupBy] = None

    def convert_to_json(
        self,
        flagged_cashier_kpis: pd.DataFrame,
        receipt_monitoring_details: pd.DataFrame,
    ) -> List[Dict[str, Any]]:
        """
        Converts two dataframes into a final list of dictionaries ready for JSON serialization.

        This is the main public method that orchestrates the conversion process.

        Args:
            flagged_cashier_kpis (pd.DataFrame): DataFrame containing flags for cashiers
                                                aggregated by week, store, and KPI.
            receipt_monitoring_details (pd.DataFrame): DataFrame containing detailed
                                                       information for individual receipts.

        Returns:
            List[Dict[str, Any]]: A nested list of dictionaries representing the structured output.
        """
        # Pre-process and group receipt details for efficient lookup
        self._prepare_receipt_details(receipt_monitoring_details)
        # Build the final nested output structure
        return self._create_output_list(flagged_cashier_kpis)

    def _prepare_receipt_details(self, df: pd.DataFrame) -> None:
        """Pre-processes the receipt details DataFrame for efficient filtering."""
        iso_dates = pd.to_datetime(df["DATUM"]).dt.isocalendar()
        df["JAHR"] = iso_dates.year
        df["KALENDARWOCHE"] = iso_dates.week
        df["FILIALNR"] = df["FILIALNR"].astype(str)
        df["BON-NR"] = df["BON-NR"].astype(str)

        if pd.api.types.is_datetime64_any_dtype(df["UHRZEIT"]):
            # Format the time correctly as HH:MM:SS
            df["UHRZEIT"] = df["UHRZEIT"].dt.strftime("%H:%M:%S")

        df["TIMESTAMP"] = df["DATUM"].astype(str) + " " + df["UHRZEIT"]
        df["KPI_ID"] = df["KENNZAHL"].map(self._parse_kpi_id_from_name)
        self._grouped_receipts = df.groupby(["JAHR", "KALENDARWOCHE", "FILIALNR", "KPI_ID"])

    def _create_output_list(self, flagged_cashier_kpis: pd.DataFrame) -> List[Dict[str, Any]]:
        """Creates the final nested list of dictionaries using a list comprehension."""
        return [
            {
                # This construct allows defining year/weeknumber for use in the nested comprehension
                "year": (year := int(year_cw_str.split("-")[0])),
                "weeknumber": (weeknumber := int(year_cw_str.split("-")[1])),
                "evaluation": [
                    {
                        "StoreID": (store := self._normalize_groupby_key(store_id_key)),
                        "KPI": [
                            kpi_dict
                            for kpi_name_key, df_per_kpi in df_per_store.groupby("KENNZAHL")
                            if (
                                kpi_dict := self._get_kpi_dict(
                                    year, weeknumber, store, self._normalize_groupby_key(kpi_name_key), df_per_kpi
                                )
                            )
                            is not None
                        ],
                    }
                    for store_id_key, df_per_store in df_per_week.groupby("FILIALNR")
                ],
            }
            # This inner loop normalizes the key once for use in the dictionary construction above
            for year_cw_key, df_per_week in flagged_cashier_kpis.groupby("WOCHE")
            for year_cw_str in [self._normalize_groupby_key(year_cw_key)]
        ]

    def _get_kpi_dict(
        self,
        year: int,
        weeknumber: int,
        store_id: str,
        kpi_name: str,
        df_per_kpi: pd.DataFrame,
    ) -> Optional[Dict[str, Any]]:
        """Constructs the dictionary for a single KPI, including its anomalies and receipts."""
        kpi_id = self._parse_kpi_id_from_name(kpi_name)
        flags = self._convert_to_flag_list(df_per_kpi)

        if not flags:
            return None

        flagged_cashiers = [flag["ServicerID"] for flag in flags]
        filtered_receipts_df = self._get_filtered_receipts(year, weeknumber, store_id, kpi_id, flagged_cashiers)

        final_receipts_df = filtered_receipts_df.rename(columns=self.config.column_mapping)[
            list(self.config.column_mapping.values())
        ]

        return {
            "KPITypeID": kpi_id,
            "Anomalies": flags,
            "Receipts": final_receipts_df.to_dict(orient="records"),
        }

    def _get_filtered_receipts(
        self,
        year: int,
        weeknumber: int,
        store_id: str,
        kpi_id: int,
        flagged_cashiers: List[int],
    ) -> pd.DataFrame:
        """Efficiently retrieves receipt data for flagged cashiers from the pre-grouped DataFrame."""
        if self._grouped_receipts is None:
            raise ValueError("Receipt details have not been prepared. Call _prepare_receipt_details first.")

        group_key = (year, weeknumber, store_id, kpi_id)
        if group_key in self._grouped_receipts.groups:
            group = self._grouped_receipts.get_group(group_key)
            return group[group["BEDIENER"].isin(flagged_cashiers)]

        return pd.DataFrame()

    @staticmethod
    def _convert_to_flag_list(df_input: pd.DataFrame) -> List[Dict[str, int]]:
        """Converts a DataFrame of flags into a list of dictionaries."""
        return df_input.rename(columns={"BEDIENER": "ServicerID", "FLAG": "AnomalyGrade"})[
            ["ServicerID", "AnomalyGrade"]
        ].to_dict(orient="records")  # type: ignore

    @staticmethod
    def _parse_kpi_id_from_name(kpi_name: str) -> int:
        """Parses the integer ID from a KPI name string (e.g., 'KPI_123: Name')."""
        return int(kpi_name.split(":")[0].removeprefix("KPI_"))

    @staticmethod
    def _normalize_groupby_key(key: Union[Any, tuple]) -> str:
        """Handles cases where a pandas groupby key can be a string or a single-element tuple."""
        return str(key[0]) if isinstance(key, tuple) else str(key)


class ExportReceiptMonitoringJson(Task):
    """Export Receipt Monitoring to JSON."""

    def __init__(self, *args, **kwargs):
        """
        Initializes the DataProcessor.

        Reads credentials but does NOT create the MyApiClient immediately.
        This ensures the constructor will not fail due to missing keys.
        """
        super().__init__(*args, **kwargs)
        self._mendix_client: Optional[MyApiClient] = None

    @property
    def mendix_client(self) -> Optional[MyApiClient]:
        """Provides access to the MyApiClient, creating it on first use."""
        # If the client already exists, return it
        if self._mendix_client:
            return self._mendix_client

        # Create and cache the client instance for future use
        try:
            logging.info("Creating MyApiClient instance for the first time.")
            self._mendix_client = MyApiClient(api_name="lchdatagate")
            return self._mendix_client
        except ValueError as e:
            # Catches errors from MyApiClient's __init__ (e.g., invalid stage)
            logging.error(f"Failed to initialize MyApiClient: {e}")
            return None

    def run_content(
        self,
        flagged_cashier_kpis: DataFrame,
        receipt_monitoring_details: DataFrame,
    ) -> Dict[str, DataFrame]:
        """Export Receipt Monitoring to JSON."""
        converter = OutputConverter(self.config)
        output = converter.convert_to_json(flagged_cashier_kpis, receipt_monitoring_details)

        for weekly_data in output:
            file_name = f"dbp_{weekly_data.get('year')}_{weekly_data.get('weeknumber')}.json"
            self.data_manager.save_json(
                output=weekly_data,
                file_name=file_name,
                project_name="digital_receipt_monitoring",
                print_pretty=True,
            )

        self._send_to_mendix(output)
        return {}

    def _send_to_mendix(self, output: List[Dict[str, Any]]) -> None:
        # Get the client instance. It will be created here if it's the first time.
        client = self.mendix_client

        # If the client could not be created, stop here.
        if not client:
            logging.error("Cannot send data to Mendix; client is not available.")
            return

        # Send data to the Mendix application
        logging.info(f"Sending {len(output)} records to Mendix...")
        for i, weekly_output in enumerate(output):
            success = client.post_json(payload=weekly_output)
            if success:
                logging.info(f"Record {i+1}/{len(output)} sent successfully.")
            else:
                failure_msg = f"Failed to send record {i+1}/{len(output)}. Check logs for details."
                if self.config.get("raise_myapi_error", True):
                    raise Exception(failure_msg)
                logging.error(failure_msg)
