from functools import reduce
from typing import Dict, List

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.window import Window

from core.task import Task


class FlagCashiers(Task):
    """
    Flags cashiers based on aggregated anomaly metrics.

    This task implements the core business logic of the pipeline. It takes the
    merged, transaction-level anomalies and applies a multi-step algorithm to
    identify cashiers for review. The process includes:
    1. Aggregating KPIs to the cashier-week level.
    2. Filtering out cashiers who don't meet minimum activity thresholds.
    3. Calculating statistical thresholds (percentiles) for each KPI.
    4. Applying hardcoded absolute thresholds for certain KPIs.
    5. Ranking cashiers within their store and region based on weighted KPIs.
    6. Applying final flags based on a combination of absolute rules and
       ranked statistical significance, respecting limits on flags per region.
    """

    @property
    def _absolute_kpi_thresholds(self) -> Dict[str, float]:
        """Gets a dictionary of KPIs and their absolute flagging thresholds from the config."""
        return {
            kpi: thresholds["absolute_kpi_threshold"]
            for kpi, thresholds in self.config.kpis.items()
            if "absolute_kpi_threshold" in thresholds
        }

    @property
    def _kpi_weighting_list(self) -> List[tuple[str, float]]:
        """Gets a list of (kpi, weight) tuples from the config for weighted ranking."""
        return [
            (
                kpi,
                kpi_weight if (kpi_weight := thresholds.get("kpi_weighting")) else self.config.default_kpi_weighting,
            )
            for kpi, thresholds in self.config.kpis.items()
        ]

    def run_content(
        self,
        receipt_monitoring_details: SparkDataFrame,
        cashier_stats: SparkDataFrame,
        vt_structure: SparkDataFrame,
    ) -> Dict[str, SparkDataFrame]:
        """
        Executes the full cashier flagging algorithm.

        Args:
            receipt_monitoring_details: A DataFrame of all detected anomalies.
            cashier_stats: A DataFrame with weekly statistics per cashier (e.g., total receipts).
            vt_structure: A DataFrame containing the store organizational hierarchy (e.g., store -> regional manager).

        Returns:
            A dictionary containing the `flagged_cashier_kpis` DataFrame with the
            final flags per cashier, week, and KPI.
        """
        # HOTFIX
        vt_structure = vt_structure.toDF(*[c.lower() for c in vt_structure.columns])

        cashier_kpis = self._agg_to_cashier_kpi_level(receipt_monitoring_details)
        cashier_kpis_w_stats = self._add_cashier_stats(cashier_kpis, cashier_stats)
        cashier_kpis_filtered = self._apply_business_rules(cashier_kpis_w_stats)
        cashier_kpis_flagged_initial = self._set_initial_kpi_flags(cashier_kpis_filtered, vt_structure)
        cashier_kpis_flagged_final = self._set_final_kpi_flags(cashier_kpis_flagged_initial)
        return {"flagged_cashier_kpis": cashier_kpis_flagged_final}

    @staticmethod
    def _agg_to_cashier_kpi_level(all_anomalies: SparkDataFrame) -> SparkDataFrame:
        """Aggregates raw anomalies to count occurrences per cashier, week, and KPI."""
        return (
            all_anomalies.withColumn(
                "WOCHE",
                F.concat_ws(
                    "-",
                    F.expr("EXTRACT(YEAROFWEEK FROM DATUM)"),
                    F.format_string("%02d", F.expr("WEEKOFYEAR(DATUM)")),
                ),
            )
            .groupBy("FILIALNR", "WOCHE", "KENNZAHL", "BEDIENER")
            .agg(F.count("BON-NR").alias("KPI_CNT"))
        )

    @staticmethod
    def _add_cashier_stats(cashier_kpis: SparkDataFrame, cashier_stats: SparkDataFrame) -> SparkDataFrame:
        """Joins the cashier KPIs with overall cashier statistics (e.g., total receipts)."""
        return (
            cashier_kpis.join(
                cashier_stats,
                (cashier_kpis["FILIALNR"] == cashier_stats["src_orig_store_nr"])
                & (cashier_kpis["BEDIENER"] == cashier_stats["cashier_id"])
                & (cashier_kpis["WOCHE"] == cashier_stats["year_week"]),
                "left",
            )
            .withColumnRenamed("total_receipts", "ANZAHL_BONS")
            .select(
                "FILIALNR",
                "WOCHE",
                "KENNZAHL",
                "BEDIENER",
                "KPI_CNT",
                "ANZAHL_BONS",
            )
        )

    def _apply_business_rules(self, input_df: SparkDataFrame) -> SparkDataFrame:
        """Filters out cashiers or KPIs that do not meet minimum activity thresholds."""
        # Filter 1: Cashier must have a minimum number of total receipts for the week.
        df_filtered = input_df.filter(F.col("ANZAHL_BONS") > self.config.min_receipts_per_cashier)

        # Filter 2: For specific KPIs, the anomaly count must meet a minimum threshold.
        min_receipts_per_kpi = self.config.get("min_receipts_per_kpi")
        if min_receipts_per_kpi:
            filter_conditions = [
                (F.col("KENNZAHL") == kpi) & (F.col("KPI_CNT") < threshold)
                for kpi, threshold in min_receipts_per_kpi.items()
            ]
            combined_condition = reduce(lambda x, y: x | y, filter_conditions)
            df_filtered = df_filtered.filter(~combined_condition)

        return df_filtered

    def _normalize_cashier_kpis(self, input_df: SparkDataFrame) -> SparkDataFrame:
        """Normalizes KPI counts by total receipts and applies configured weights."""
        kpi_weighting_df = self.data_manager.spark.createDataFrame(self._kpi_weighting_list, ["KENNZAHL", "WEIGHT"])

        return (
            input_df.join(kpi_weighting_df, on="KENNZAHL", how="left")
            .withColumn("KPI_NORMIERT", (F.col("KPI_CNT") / F.col("ANZAHL_BONS")) * 100)
            .withColumn("KPI_WEIGHTED", F.col("KPI_NORMIERT") * F.col("WEIGHT"))
            .drop("WEIGHT")
        )

    def _add_vt_structure(self, input_df: SparkDataFrame, vt_structure: SparkDataFrame) -> SparkDataFrame:
        """Adds organizational structure (e.g., regional manager) and calculates flag limits."""
        # Calculate the max number of red flags allowed per regional manager.
        rl_window = Window.partitionBy("REGIONALLEITER")
        vt_structure_with_limits = vt_structure.withColumn(
            "MAX_RED_FLAGS",
            (self.config.avg_red_flags_per_store_per_rl * F.count("ORIG_STORE_NR").over(rl_window)).cast("int"),
        )

        return input_df.join(
            vt_structure_with_limits,
            input_df.FILIALNR == vt_structure_with_limits.orig_store_nr,
            how="left",
        )

    def _set_absolute_flags(self, input_df: SparkDataFrame) -> SparkDataFrame:
        """Sets a flag for any KPI that exceeds a hardcoded absolute threshold."""
        if not self._absolute_kpi_thresholds:
            return input_df.withColumn("ABS_FLAG", F.lit(None))

        flag_conditions = [
            (F.col("KENNZAHL") == kpi) & (F.col("KPI_CNT") >= threshold)
            for kpi, threshold in self._absolute_kpi_thresholds.items()
        ]
        combined_condition = reduce(lambda x, y: x | y, flag_conditions)

        return input_df.withColumn("ABS_FLAG", F.when(combined_condition, 1).otherwise(None))

    @staticmethod
    def _add_ranking(
        input_df: SparkDataFrame, ranking_cols: List[str], sorting_cols: List[str], col_name: str
    ) -> SparkDataFrame:
        """A generic helper to add a ranking column based on a window specification."""
        window_spec = Window.partitionBy(*ranking_cols).orderBy(*[F.col(c).desc() for c in sorting_cols])
        return input_df.withColumn(col_name, F.rank().over(window_spec))

    def _get_threshold_df(self, input_df: SparkDataFrame) -> SparkDataFrame:
        """
        Calculates and validates the statistical thresholds for flagging.

        This method orchestrates the three main steps:
        1. Build the dynamic percentile aggregation expressions.
        2. Execute the aggregation against the input data.
        3. Apply robustness rules to the resulting thresholds.
        """
        agg_expressions = self._build_percentile_agg_expressions()
        raw_thresholds_df = self._calculate_raw_thresholds(input_df, agg_expressions)
        final_thresholds_df = self._apply_threshold_robustness_rules(raw_thresholds_df)

        return final_thresholds_df

    def _build_percentile_agg_expressions(self) -> Dict[str, F.Column]:
        """
        Builds the dynamic PySpark Column expressions for percentile calculations.

        Returns:
            A dictionary containing the aliased Column expressions for orange
            and red thresholds.
        """

        def get_percentile_expr(percentile_value: float) -> str:
            """Builds a percentile approximation SQL expression."""
            return f"percentile_approx(KPI_NORMIERT, {round((100 - float(percentile_value)) / 100, 2)})"

        excluded = list(self._absolute_kpi_thresholds.keys())
        red_conditions = F.when(F.col("KENNZAHL").isin(excluded), None)
        orange_conditions = None

        for kpi, thresholds in self.config.kpis.items():
            if orange_percentile := thresholds.get("orange_percentile"):
                expr = get_percentile_expr(orange_percentile)
                if orange_conditions is None:
                    orange_conditions = F.when(F.col("KENNZAHL") == kpi, F.expr(expr))
                else:
                    orange_conditions = orange_conditions.when(F.col("KENNZAHL") == kpi, F.expr(expr))
            if red_percentile := thresholds.get("red_percentile"):
                expr = get_percentile_expr(red_percentile)
                red_conditions = red_conditions.when(F.col("KENNZAHL") == kpi, F.expr(expr))

        default_orange_expr = F.expr(get_percentile_expr(self.config.default_orange_percentile))
        default_red_expr = F.expr(get_percentile_expr(self.config.default_red_percentile))

        final_orange_conditions = (
            default_orange_expr if orange_conditions is None else orange_conditions.otherwise(default_orange_expr)
        )
        final_red_conditions = red_conditions.otherwise(default_red_expr)

        return {
            "ORANGE_THRESHOLD": final_orange_conditions.alias("ORANGE_THRESHOLD"),
            "RED_THRESHOLD": final_red_conditions.alias("RED_THRESHOLD"),
        }

    def _calculate_raw_thresholds(
        self, input_df: SparkDataFrame, agg_expressions: Dict[str, F.Column]
    ) -> SparkDataFrame:
        """Executes the groupBy and agg operations to calculate raw thresholds and population sizes."""
        return input_df.groupBy("WOCHE", "KENNZAHL").agg(
            agg_expressions["ORANGE_THRESHOLD"],
            agg_expressions["RED_THRESHOLD"],
            F.count("BEDIENER").alias("POPULATION_SIZE"),
        )

    def _apply_threshold_robustness_rules(self, thresholds_df: SparkDataFrame) -> SparkDataFrame:
        """
        Applies business rules to the calculated thresholds to handle edge cases.

        - Nullifies thresholds if the population size is too small.
        - Nullifies thresholds if the calculated value is zero.
        """
        min_population = self.config.get("min_population_for_statistical_flag", 10)

        return thresholds_df.withColumn(
            "ORANGE_THRESHOLD",
            F.when(
                (F.col("POPULATION_SIZE") >= min_population) & (F.col("ORANGE_THRESHOLD") > 0),
                F.col("ORANGE_THRESHOLD"),
            ).otherwise(None),
        ).withColumn(
            "RED_THRESHOLD",
            F.when(
                (F.col("POPULATION_SIZE") >= min_population) & (F.col("RED_THRESHOLD") > 0), F.col("RED_THRESHOLD")
            ).otherwise(None),
        )

    def _set_initial_kpi_flags(self, input_df: SparkDataFrame, vt_structure: SparkDataFrame) -> SparkDataFrame:
        """Applies absolute and statistical thresholds to set an initial flag level (1, 2, or 3)."""
        cashier_kpis_normed = self._normalize_cashier_kpis(input_df)
        thresholds_df = self._get_threshold_df(cashier_kpis_normed)
        df_abs_flags = self._set_absolute_flags(cashier_kpis_normed)
        df_w_thresholds = df_abs_flags.join(thresholds_df, on=["WOCHE", "KENNZAHL"], how="left")
        df_w_rl = self._add_vt_structure(df_w_thresholds, vt_structure)

        # Set initial flag: 3 for absolute, 2 for red, 1 for orange
        return df_w_rl.withColumn(
            "INITIAL_FLAG",
            F.when(F.col("ABS_FLAG") == 1, 3)
            .when(F.col("KPI_NORMIERT") >= F.col("RED_THRESHOLD"), 2)
            .when(F.col("KPI_NORMIERT") >= F.col("ORANGE_THRESHOLD"), 1)
            .otherwise(None),
        )

    def _set_final_kpi_flags(self, input_df: SparkDataFrame) -> SparkDataFrame:
        """
        Applies ranking and flag limits to convert initial flags into the final flags (1 or 2).

        This step ensures that only the top-ranked cashiers are flagged, respecting the
        limits configured per store and per regional manager.
        """
        # Rank cashiers within their region to identify the top candidates for red flags
        df_w_ranking = self._add_ranking(
            input_df,
            ranking_cols=["WOCHE", "REGIONALLEITER"],
            sorting_cols=["INITIAL_FLAG", "KPI_WEIGHTED"],
            col_name="RED_FLAG_RANK",
        )
        # Rank cashiers within their store for orange flags
        df_w_ranking = self._add_ranking(
            df_w_ranking,
            ranking_cols=["WOCHE", "FILIALNR"],
            sorting_cols=["INITIAL_FLAG", "KPI_WEIGHTED"],
            col_name="ORANGE_FLAG_RANK",
        )

        # Final flagging logic
        # A red flag (2) is given for absolute violations OR if the cashier is ranked high enough in their region.
        # An orange flag (1) is given if the cashier is ranked high enough in their store.
        return (
            df_w_ranking.withColumn(
                "FLAG",
                F.when(
                    (F.col("INITIAL_FLAG") == 3)
                    | ((F.col("INITIAL_FLAG") == 2) & (F.col("RED_FLAG_RANK") <= F.col("MAX_RED_FLAGS"))),
                    2,
                )
                .when(F.col("ORANGE_FLAG_RANK") <= self.config.num_flags_per_store, 1)
                .otherwise(None),
            )
            .select("WOCHE", "FILIALNR", "BEDIENER", "KENNZAHL", "FLAG")
            .dropna(subset="FLAG")
        )
