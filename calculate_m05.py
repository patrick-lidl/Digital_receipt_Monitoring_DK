"""Merge & process receipt metrics."""

from typing import Dict

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from core.task import Task


class MergeReceiptAnomalies(Task):
    """Merge & process receipt metrics."""

    def run_content(
        self,
        dbp_kpi_01: SparkDataFrame,
        dbp_kpi_02: SparkDataFrame,
        dbp_kpi_03: SparkDataFrame,
        dbp_kpi_04: SparkDataFrame,
        dbp_kpi_05: SparkDataFrame,
        dbp_kpi_06: SparkDataFrame,
        dbp_kpi_07: SparkDataFrame,
        dbp_kpi_08: SparkDataFrame,
        dbp_kpi_09: SparkDataFrame,
        dbp_kpi_10: SparkDataFrame,
    ) -> Dict[str, SparkDataFrame]:
        """Merge & process receipt metrics."""
        all_kpi_dfs = [
            dbp_kpi_01,
            dbp_kpi_02,
            dbp_kpi_03,
            dbp_kpi_04,
            dbp_kpi_05,
            dbp_kpi_06,
            dbp_kpi_07,
            dbp_kpi_08,
            dbp_kpi_09,
            dbp_kpi_10,
        ]

        all_kpi_names = list(self.config.kpis.keys())

        # Initialize all_anomalies with the first DataFrame and add the kpi column
        all_anomalies = all_kpi_dfs[0].withColumn("kpi", F.lit(all_kpi_names[0]))

        # Loop through the remaining DataFrames and union them
        for df, kpi_name in zip(all_kpi_dfs[1:], all_kpi_names[1:]):
            all_anomalies = all_anomalies.union(df.withColumn("kpi", F.lit(kpi_name)))

        all_anomalies = (
            all_anomalies
            # rename columns
            .withColumnRenamed("src_orig_store_nr", "FILIALNR")
            .withColumnRenamed("receipt_dt", "DATUM")
            .withColumnRenamed("receipt_tmsp", "UHRZEIT")
            .withColumnRenamed("register_id", "KASSE")
            .withColumnRenamed("cashier_id", "BEDIENER")
            .withColumnRenamed("receipt_id", "BON-NR")
            .withColumnRenamed("kpi", "KENNZAHL")
            # reformat column type
            .withColumn("UHRZEIT", F.date_format(F.to_timestamp("UHRZEIT"), "HH:mm:ss"))
            .withColumn("FILIALNR", F.col("FILIALNR").cast("int"))
            .withColumn("KASSE", F.col("KASSE").cast("int"))
            .withColumn("BEDIENER", F.col("BEDIENER").cast("int"))
            .withColumn("BON-NR", F.col("BON-NR").cast("int"))
            # select output columns
            .select(
                "FILIALNR",
                "DATUM",
                "UHRZEIT",
                "KASSE",
                "BEDIENER",
                "BON-NR",
                "KENNZAHL",
            )
        )
        return {"receipt_monitoring_details": all_anomalies}
