import logging
from typing import List, Optional, Tuple

import pandas as pd

KPIS = [
    "KPI_01: Bons ausserhalb der Oeffnungszeiten inkl. Toleranz",
    "KPI_02: Rueckgaben ausserhalb der Oeffnungszeiten",
    "KPI_03: Anzahl Zeilenstornos",
    "KPI_04: Bons mit Zeilenstornos bis auf 1 Artikel",
    "KPI_05: Bons mit Zeilenstornos nach der letzten Zwischensumme",
    "KPI_06: Bons mit Zeilenstorno des letzten Artikels",
    "KPI_07: Rueckgaben ohne Autorisierung",
    "KPI_08: Rueckgaben allgemein",
    "KPI_09: Rueckgaben ohne Original-Beleg",
    "KPI_10: Bonabbrueche",
]


class KpiMonitoringData:
    """Process, analyze, and store historical receipt monitoring data, including flag statistics."""

    def __init__(
        self,
        flagged_cashier_kpis: pd.DataFrame,
        receipt_monitoring_details: pd.DataFrame,
        vt_structure: pd.DataFrame,
        look_back_weeks: int = 8,
    ) -> None:
        """Initializes the KpiMonitoringData object by preprocessing and validating the input data."""
        # This call correctly uses your new, robust helper method
        vt_aug, receipts_aug, flags_aug = self._validate_and_prepare_sources(
            vt_structure.copy(), receipt_monitoring_details.copy(), flagged_cashier_kpis.copy()
        )

        # All the logic below remains the same and now works with the more reliable data
        vt_weeks = set(vt_aug["WOCHE"].unique())
        receipt_weeks = set(receipts_aug["WOCHE"].unique())
        flag_weeks = set(flags_aug["WOCHE"].unique())
        common_weeks = sorted(list(vt_weeks & receipt_weeks & flag_weeks))

        self.weeks_in_data: List[str] = common_weeks[-look_back_weeks:]

        vt_final = vt_aug[vt_aug["WOCHE"].isin(self.weeks_in_data)]
        receipts_final = receipts_aug[receipts_aug["WOCHE"].isin(self.weeks_in_data)]
        flags_final = flags_aug[flags_aug["WOCHE"].isin(self.weeks_in_data)]

        self.stores_per_rl = self._extract_store_rl_mapping(vt_final)
        self.anomalies = self._extract_anomalies(receipts_final)
        self.flags = self._extract_flags(flags_final)

        self._total_flag_stats: Optional[pd.DataFrame] = None
        self._weekly_flag_stats: Optional[pd.DataFrame] = None

    # ----------------------------------------------------------------------------------
    # Public Methods for Data Retrieval
    # ----------------------------------------------------------------------------------

    def get_total_flag_stats(self) -> pd.DataFrame:
        """Returns the overall (aggregated) flag statistics across all weeks."""
        if self._total_flag_stats is None:
            self._total_flag_stats = self._calculate_flag_stats()
        return self._total_flag_stats

    def get_weekly_flag_stats(self) -> pd.DataFrame:
        """Returns flag statistics computed separately for each week."""
        if self._weekly_flag_stats is None:
            self._weekly_flag_stats = self._calculate_flag_stats_weekly()
        return self._weekly_flag_stats

    def get_flag_counts(self, agg_level: str) -> pd.DataFrame:
        """
        Calculates the count of red, orange, and all flags per week at a given aggregation level.

        This method leverages the helper function to ensure zero counts are included and pivots
        the data to return a wide-format DataFrame with separate columns for each flag type's count.

        Args:
            agg_level (str): The level to aggregate on (e.g., 'REGIONALLEITER', 'FILIALNR').

        Returns:
            pd.DataFrame: A DataFrame with columns [WOCHE, agg_level, count_rot, count_orange, count_alle].
        """
        # 1. Start with the complete, zero-filled counts from the helper method.
        counts = self._calculate_flags_counts_per_week()

        if agg_level not in counts.columns:
            raise ValueError(f"Aggregation level '{agg_level}' not found in the data.")

        # 2. Aggregate counts for each specific FLAG (1 or 2) at the desired level.
        agg_counts = counts.groupby(["WOCHE", agg_level, "FLAG"], as_index=False)["count"].sum()

        # 3. Pivot the table to transform FLAG values (1, 2) into separate columns.
        final_df = agg_counts.pivot_table(
            index=["WOCHE", agg_level], columns="FLAG", values="count", fill_value=0
        ).reset_index()

        # 4. Ensure columns for both flag types exist, even if one type never occurs.
        if 1 not in final_df.columns:
            final_df[1] = 0
        if 2 not in final_df.columns:
            final_df[2] = 0

        # 5. Rename columns for clarity (2 -> rot, 1 -> orange) and calculate the total.
        final_df = final_df.rename(columns={2: "count_rot", 1: "count_orange"})
        final_df["count_alle"] = final_df["count_rot"] + final_df["count_orange"]

        return final_df

    def get_bons_with_anomalies_per_week(self) -> pd.DataFrame:
        """Return series with weekly stats on receipts with anomalies."""
        return self.anomalies[["WOCHE", "BON-NR"]].drop_duplicates().value_counts("WOCHE").sort_index().reset_index()

    def get_anomalies_per_week(self, per_kpi: bool = True) -> pd.DataFrame:
        """Return series with weekly anomaly stats."""
        anomalies_per_week = self.anomalies[["WOCHE", "KENNZAHL"]].value_counts().sort_index().reset_index()
        if not per_kpi:
            anomalies_per_week = anomalies_per_week.groupby("WOCHE")["count"].sum().sort_index().reset_index()
        return anomalies_per_week

    def get_anomalies_per_store(self) -> pd.DataFrame:
        """Helper to get anomaly counts per store."""
        return self.anomalies.groupby(["WOCHE", "FILIALNR"]).size().reset_index(name="ANZAHL")

    def get_per_store_flag_stats(self) -> pd.DataFrame:
        """Calculates flag statistics at the store-level, suitable for distribution analysis."""
        df = self.get_total_flag_stats()
        id_vars = ["Metrik"]
        value_vars = [col for col in df.columns if "KPI_" in col or col == "GESAMT"]
        melted_df = df.melt(id_vars=id_vars, value_vars=value_vars, var_name="KENNZAHL", value_name="Value")
        return melted_df

    def get_anomaly_overview_df(self) -> pd.DataFrame:
        """Return an overview of anomaly counts."""
        count_df = (
            self.anomalies[["KENNZAHL", "BON-NR"]]
            .drop_duplicates()
            .value_counts("KENNZAHL")
            .sort_index()
            .to_frame(name="Anzahl Bons")
        )
        count_df["Anteil [%]"] = round(100 * count_df["Anzahl Bons"] / count_df["Anzahl Bons"].sum(), 2)
        count_df["Anzahl Bons"] = count_df["Anzahl Bons"].astype(int).astype(str)
        count_df = count_df.T
        count_df.columns = [f"KPI_{i:02d}" for i in range(1, 11)]
        return count_df

    def get_flag_distribution_over_kpis(self) -> pd.DataFrame:
        """Return an overview of flag counts distribution across KPIs."""
        df = self.get_total_flag_stats()
        # Clean KPI names like "KPI_01: Description" to "KPI_01"
        df.columns = [col.split(":")[0] for col in df.columns]
        kpi_list = [f"KPI_{str(idx).zfill(2)}" for idx in range(1, 11)]

        df_filtered = df[
            df["Metrik"].isin(["Anzahl Bediener rot | Landesweit", "Anzahl Bediener orange | Landesweit"])
        ].reset_index(drop=True)

        df_filtered.insert(2, "Durchschnitt", round(df_filtered[kpi_list].mean(axis=1), 2))
        df_filtered.insert(3, "Zielwert", [130, 56])
        df_filtered.insert(
            4,
            "MAPE",
            round(
                df_filtered[kpi_list]
                .sub(df_filtered["Zielwert"], axis=0)
                .div(df_filtered["Zielwert"], axis=0)
                .abs()
                .mean(axis=1)
                * 100,
                2,
            ),
        )
        return df_filtered.rename(columns={"GESAMT": "Summe", "Metrik": ""}).set_index("")

    # ----------------------------------------------------------------------------------
    # Internal Methods for Flag Statistics
    # ----------------------------------------------------------------------------------

    def _calculate_flags_counts_per_week(self) -> pd.DataFrame:
        """
        Calculates the raw count of flags per week, store, KPI, and flag type.

        FIXME: cache this dataframe
        """
        group_cols = ["WOCHE", "REGIONALLEITER", "FILIALNR", "KENNZAHL", "FLAG"]
        counts = self.flags.groupby(group_cols).size().reset_index(name="count")

        # Create a new index base to include zeros for all combinations
        new_index_base = self._add_and_explode(self.stores_per_rl, "KENNZAHL", KPIS)
        new_index = self._add_and_explode(new_index_base, "FLAG", [1, 2])

        # Reindex to fill in zeros for stores/kpis with no flags
        counts = pd.merge(counts, new_index, on=group_cols, how="right")
        counts["count"] = counts["count"].fillna(0).astype(int)

        return counts

    def _create_stats_pivot(self, counts_df: pd.DataFrame, value_col: str, weekly: bool) -> pd.DataFrame:
        """
        Generic helper to calculate statistics and pivot the data.

        Args:
            counts_df: DataFrame containing the counts to be aggregated.
            value_col: The name of the column with values to aggregate.
            weekly: If True, calculations are grouped by "WOCHE".
        """
        # 1. Add 'GESAMT' (total) KPI row
        gesamt_group_cols = [col for col in counts_df.columns if col not in ["KENNZAHL", value_col, "FLAG"]]
        counts_with_gesamt = self._add_gesamt_rows(counts_df, gesamt_group_cols + ["FLAG"], "KENNZAHL", value_col)

        # 2. Define aggregation configuration
        agg_config = {
            "sum": (value_col, "sum"),
            "mean": (value_col, "mean"),
            "Median": (value_col, "median"),
            "Max": (value_col, "max"),
            "Min": (value_col, "min"),
        }

        # 3. Define grouping columns based on whether it's a weekly report
        stats_group_cols = ["KENNZAHL", "FLAG"]
        if weekly:
            stats_group_cols.insert(0, "WOCHE")

        # 4. Calculate statistics per store ("Filiale") and nationwide
        stats_per_store = counts_with_gesamt.groupby(stats_group_cols).agg(**agg_config)
        stats_per_store.columns = [f"{col} Filiale" for col in stats_per_store.columns]
        stats_per_store.rename(columns={"sum Filiale": "sum Landesweit"}, inplace=True)

        # 5. Calculate statistics per regional manager ("RL")
        counts_per_rl = counts_with_gesamt.groupby(stats_group_cols + ["REGIONALLEITER"])[value_col].sum().reset_index()
        stats_per_rl = counts_per_rl.groupby(stats_group_cols).agg(
            **{k: v for k, v in agg_config.items() if k != "sum"}  # Exclude sum for RL stats
        )
        stats_per_rl.columns = [f"{col} RL" for col in stats_per_rl.columns]

        # 6. Combine store and RL stats
        final_stats = pd.concat([stats_per_store, stats_per_rl], axis=1).reset_index()

        # 7. Reshape (melt) data into a long format for pivoting
        id_vars = ["FLAG", "KENNZAHL"]
        if weekly:
            id_vars.insert(0, "WOCHE")

        final_stats_melted = final_stats.melt(id_vars=id_vars, var_name="stat_name", value_name="value")

        # Create the final metric description
        final_stats_melted["Metrik"] = (
            "Anzahl Bediener "
            + final_stats_melted["FLAG"].map({1: "orange", 2: "rot"})
            + " | "
            + final_stats_melted["stat_name"].str.replace("sum ", "").str.replace("mean", "⌀")
        )

        # 8. Pivot the table to its final wide format
        pivot_index = ["Metrik", "WOCHE"] if weekly else "Metrik"
        df_pivot = (
            final_stats_melted.pivot_table(index=pivot_index, columns="KENNZAHL", values="value").round(3).reset_index()
        )
        df_pivot.columns.name = None

        return df_pivot

    def _calculate_flag_stats(self) -> pd.DataFrame:
        """Calculates aggregated flag statistics across all weeks."""
        counts_per_week = self._calculate_flags_counts_per_week()

        # Aggregate weekly counts into a single average value per store
        group_cols = ["REGIONALLEITER", "FILIALNR", "KENNZAHL", "FLAG"]
        counts_agg = (
            counts_per_week.groupby(group_cols)["count"]
            .mean()
            .reset_index()
            .rename(columns={"count": "ANZAHL_PRO_WOCHE"})
        )

        # Call the generic helper with the aggregated data
        return self._create_stats_pivot(counts_agg, value_col="ANZAHL_PRO_WOCHE", weekly=False)

    def _calculate_flag_stats_weekly(self) -> pd.DataFrame:
        """Calculates flag statistics on a per-week basis."""
        counts_per_week = self._calculate_flags_counts_per_week()

        # Call the generic helper directly with the weekly data
        return self._create_stats_pivot(counts_per_week, value_col="count", weekly=True)

    # ----------------------------------------------------------------------------------
    # Internal Helper and Pre-processing Methods
    # ----------------------------------------------------------------------------------

    @staticmethod
    def _validate_and_prepare_sources(
        vt_structure: pd.DataFrame,
        receipt_monitoring_details: pd.DataFrame,
        flagged_cashier_kpis: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Validates sources and adds a consistent 'WOCHE' column by inferring it from reliable data."""
        # --- 1. Create a Reliable run_id -> WOCHE Map from Transactional Data ---
        # From receipt_monitoring_details (has reliable DATUM)
        receipt_map = receipt_monitoring_details[["run_id", "DATUM"]].drop_duplicates("run_id")
        receipt_map["WOCHE"] = pd.to_datetime(receipt_map["DATUM"]).dt.strftime("%G-%V")

        # From flagged_cashier_kpis (has reliable WOCHE)
        flag_map = flagged_cashier_kpis[["run_id", "WOCHE"]].drop_duplicates("run_id")

        # Combine the reliable sources
        reliable_map = pd.concat([receipt_map[["run_id", "WOCHE"]], flag_map]).drop_duplicates()

        # Validate that the reliable sources are consistent
        inconsistent_runs = reliable_map.groupby("run_id")["WOCHE"].nunique()
        inconsistent_runs = inconsistent_runs[inconsistent_runs > 1]
        if not inconsistent_runs.empty:
            logging.warning(
                "Data Consistency Issue: The following run_ids map to multiple "
                + f"weeks in the source data: {inconsistent_runs.index.tolist()}"
            )

        # Create the final, definitive mapping dataframe
        run_week_df = reliable_map.drop_duplicates("run_id")

        # --- 2. Augment All DataFrames with the Reliable WOCHE ---
        # The original WOCHE column in flags is dropped to ensure it's replaced by our consistent map.
        # We ignore errors in case a WOCHE column doesn't exist.
        vt_aug = pd.merge(vt_structure, run_week_df, on="run_id")
        receipts_aug = pd.merge(receipt_monitoring_details, run_week_df, on="run_id")
        flags_aug = pd.merge(flagged_cashier_kpis.drop(columns="WOCHE", errors="ignore"), run_week_df, on="run_id")

        # --- 3. Perform Final Validation on Augmented DataFrames ---
        for name, df in {"vt_structure": vt_aug, "receipt_details": receipts_aug, "flagged_kpis": flags_aug}.items():
            week_to_run_counts = df.groupby("WOCHE")["run_id"].nunique()
            inconsistent_weeks = week_to_run_counts[week_to_run_counts > 1]

            if not inconsistent_weeks.empty:
                warning_messages = []
                for week, count in inconsistent_weeks.items():
                    problematic_runs = df[df["WOCHE"] == week]["run_id"].unique().tolist()
                    warning_messages.append(f"  - Week '{week}': Found {count} run_ids {problematic_runs}")
                logging.warning(f"Validation failed in '{name}' after mapping:\n" + "\n".join(warning_messages))

        return vt_aug, receipts_aug, flags_aug

    @staticmethod
    def _extract_store_rl_mapping(vt_structure_aug: pd.DataFrame) -> pd.DataFrame:
        """Extracts a weekly mapping from the augmented vt_structure DataFrame."""
        # The WOCHE column is now pre-calculated and consistent
        return (
            vt_structure_aug[["WOCHE", "ORIG_STORE_NR", "REGIONALLEITER"]]
            .drop_duplicates()
            .rename(columns={"ORIG_STORE_NR": "FILIALNR"})
        )

    def _extract_anomalies(self, receipt_monitoring_details_aug: pd.DataFrame) -> pd.DataFrame:
        """Extracts anomalies and merges them with RL information."""
        # The WOCHE column is now pre-calculated
        return (
            receipt_monitoring_details_aug[["WOCHE", "FILIALNR", "KENNZAHL", "BEDIENER", "BON-NR"]]
            .drop_duplicates()
            .merge(self.stores_per_rl, on=["WOCHE", "FILIALNR"], how="inner")
        )

    def _extract_flags(self, flagged_cashier_kpis_aug: pd.DataFrame) -> pd.DataFrame:
        """Merges flagged KPIs with RL information using the consistent week data."""
        return flagged_cashier_kpis_aug.merge(self.stores_per_rl, on=["WOCHE", "FILIALNR"], how="inner")

    @staticmethod
    def _add_and_explode(input_df: pd.DataFrame, col_name: str, unique_values: list) -> pd.DataFrame:
        """Adds a new column by creating all combinations with a list of unique values."""
        return (
            input_df.assign(key=1)
            .merge(pd.DataFrame({col_name: unique_values, "key": 1}), on="key")
            .drop("key", axis=1)
        )

    @staticmethod
    def _add_gesamt_rows(
        input_df: pd.DataFrame, group_by_cols: List[str], output_col: str, value_col: str
    ) -> pd.DataFrame:
        """Adds a 'GESAMT' row by summing the value column across the specified groups."""
        gesamt_counts = input_df.groupby(group_by_cols, as_index=False)[value_col].sum()
        gesamt_counts[output_col] = "GESAMT"
        return pd.concat([input_df, gesamt_counts], ignore_index=True)
