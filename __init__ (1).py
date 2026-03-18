# Digital Receipt Monitoring Pipeline 🧾

> This project is an end-to-end pipeline designed to monitor digital receipts for anomalies and flag potentially problematic cashier behavior for review.

## Core Objective

The primary objective of this pipeline is to automatically detect patterns that may indicate fraud, operational errors, or a need for cashier training. It does this by ingesting raw receipt data, calculating a series of risk metrics, and aggregating them to flag specific cashiers who exceed certain thresholds. The final output is a JSON file for system integration and a monitoring report for human review.

The entire pipeline runs as a weekly scheduled job on Databricks.

---
## Pipeline Workflow & Structure

The pipeline is organized as a single `Job` (`digital_receipt_monitoring`) composed of multiple `Stages`, which in turn contain the individual `Task`s. The code is structured to mirror this workflow in numbered directories, making the data flow easy to follow.

### Stage 01: Data Preparation (`tasks/stage_01_prep/`)
This initial stage is responsible for all ETL (Extract, Transform, Load) operations. It takes raw data from source systems—loaded automatically by `DataLoader` tasks—and transforms it into clean, structured features. Key tasks in this stage include:
* `ReceiptsOutsideStoreHours`
* `CashierStats`
* `ReceiptVoidsCashOnly`

### Stage 02: Metric Calculation (`tasks/stage_02_metrics/`)
This is the core analysis stage of the pipeline. It consists of ten separate `MetricTask`s (`CalculateM01` through `CalculateM10`), each responsible for calculating a single, specific anomaly indicator. These metrics are designed to pinpoint different types of suspicious activities.

### Stage 03: Aggregation & Flagging (`tasks/stage_03_aggregation/`)
This stage takes the outputs of the ten individual metric tasks and combines them into a single, unified dataset.
* `MergeReceiptAnomalies`: Gathers all metric outputs.
* `FlagCashiers`: The core logic of the project resides here. It applies a set of business rules and thresholds to the aggregated metrics to produce the final list of flagged cashiers.

### Stage 04: Output (`tasks/stage_04_output/`)
The final stage is responsible for generating the pipeline's outputs.
* `ExportReceiptMonitoringJson`: Creates a structured JSON file containing the results for downstream consumption.
* `CreateWeeklyReport`: Generates a human-readable report or dashboard summarizing the week's findings.
* `JobPostProcessing`: TODO

---
## Configuration & Dependencies

* The complete job, including its weekly schedule and the full list of tasks, is defined in `configs/jobs/digital_receipt_monitoring.yml`.
* Each task is registered with its module path in `configs/tasks.yml`.
* Note that this job also utilizes tasks from other projects, such as `HolidayStoreHours` and `OpeningHoursCalendar` from the `store_opening_hours` package, to provide necessary business context.

---
## Overview of the Flagging Logic

The `FlagCashiers` task is the final and most complex step in the monitoring pipeline. Its goal is to transform the raw anomaly counts for each cashier into a final, actionable set of **Orange Flags (Grade 1)** and **Red Flags (Grade 2)**.

It does this using a multi-stage algorithm that combines:
* **Absolute Rules**: Hardcoded thresholds for specific KPIs (e.g., "more than 5 of X is always a red flag").
* **Statistical Analysis**: Dynamic thresholds based on a cashier's behavior relative to their peers for a given week (e.g., "in the top 5% for Y").
* **Ranking & Limiting**: A system to rank the most significant anomalies and limit the total number of flags generated per store and per region to ensure focus.

---
## Configuration Parameters

The entire algorithm is controlled by a set of parameters, typically defined in `configs/tasks/flag_cashiers.yml` and potentially overridden in a job config.

### General Settings
* `min_receipts_per_cashier` (Integer): The minimum number of total receipts a cashier must have in a week to be considered for flagging. **Implication**: This reduces noise by excluding cashiers with very low activity.
* `num_flags_per_store` (Integer): The maximum **rank** for an orange flag within a single store. **Implication**: This limits the number of orange flags per store, focusing on the top `N` anomalies. Note that ties in rank can cause this number to be exceeded.
* `avg_red_flags_per_store_per_rl` (Integer/Float): The average number of red flags you expect per store within a regional manager's (RL) area. This is used to calculate the total red flag limit for the entire region. **Implication**: This controls the overall volume of high-priority red flags across a region.
* `default_kpi_weighting` (Float): The default weight applied to a KPI's normalized score during ranking if no specific weight is defined.
* `default_orange_percentile` (Float): The default percentile (e.g., `90.0`) for the orange flag threshold if not specified per KPI.
* `default_red_percentile` (Float): The default percentile (e.g., `95.0`) for the red flag threshold if not specified per KPI.

### Per-KPI Settings
These settings are defined within the `kpis` dictionary, where each key is a KPI name (e.g., "KPI_01").

* `min_receipts_per_kpi` (Dictionary): A dictionary mapping specific KPIs to a minimum raw count. Any cashier who has fewer than this count for that specific KPI is excluded from the analysis for that KPI. **Implication**: This prevents flagging based on a single, insignificant occurrence of a specific anomaly.
* `absolute_kpi_threshold` (Integer): If a cashier's raw count (`KPI_CNT`) for this KPI is greater than or equal to this value, they are immediately given the highest priority flag. **Implication**: This is for "zero tolerance" rules that bypass statistical comparison.
* `kpi_weighting` (Float): A multiplier applied to the normalized score of this KPI. **Implication**: Allows you to make certain KPIs more influential in the final ranking (e.g., a high-risk anomaly can be given a weight of `1.5` to push it up the ranks).
* `orange_percentile` (Float): The specific percentile for an orange flag for this KPI (e.g., `90.0` means a cashier must be in the top 10% of their peers).
* `red_percentile` (Float): The specific percentile for a red flag for this KPI (e.g., `98.0` means a cashier must be in the top 2%).

---
## The Flagging Algorithm: Step-by-Step

The `run_content` method executes the following sequence:

### Step 1: Aggregate Anomalies
* **Method**: `_agg_to_cashier_kpi_level`
* **What it does**: It takes the raw, transaction-level anomalies and counts them, grouping by cashier, store, week, and KPI type.
* **Result**: A DataFrame with the raw anomaly count (`KPI_CNT`) for every cashier and every KPI they triggered.

### Step 2: Add Cashier Stats
* **Method**: `_add_cashier_stats`
* **What it does**: It joins the result from Step 1 with the `cashier_stats` data to get the total number of receipts (`ANZAHL_BONS`) processed by each cashier in that week.
* **Result**: The same DataFrame as before, but now with the context of the cashier's total activity level.

### Step 3: Apply Business Rules
* **Method**: `_apply_business_rules`
* **What it does**: It filters out records that are not significant enough for analysis.
* **Logic**:
    * It removes any cashier whose total weekly receipts (`ANZAHL_BONS`) is less than `min_receipts_per_cashier`.
    * For specific KPIs defined in `min_receipts_per_kpi`, it removes records where the anomaly count (`KPI_CNT`) is below the configured minimum.
* **Result**: A cleaner dataset focused on statistically relevant activity.

### Step 4: Set Initial Flags
* **Method**: `_set_initial_kpi_flags`
* **What it does**: This is a multi-part step that calculates and applies all the statistical and absolute rules to assign a preliminary flag level.
    1.  **Normalize & Weight**: It calculates `KPI_NORMIERT` (the percentage of a cashier's receipts that had this anomaly) and `KPI_WEIGHTED` (the normalized value multiplied by the `kpi_weighting`).
    2.  **Calculate Thresholds**: For each KPI and week, it calculates the statistical `ORANGE_THRESHOLD` and `RED_THRESHOLD` by finding the value at the configured percentile (e.g., 90th, 95th) across all cashiers.
    3.  **Apply Flags**: It creates an `INITIAL_FLAG` column with a priority level:
        * **Level 3 (Highest)**: If the raw `KPI_CNT` exceeds the `absolute_kpi_threshold`.
        * **Level 2**: If the `KPI_NORMIERT` exceeds the `RED_THRESHOLD`.
        * **Level 1**: If the `KPI_NORMIERT` exceeds the `ORANGE_THRESHOLD`.

### Step 5: Set Final Flags
* **Method**: `_set_final_kpi_flags`
* **What it does**: This final step converts the `INITIAL_FLAG` into the final Orange (1) or Red (2) flag by applying ranking and limits.
    1.  **Ranking**: It calculates two ranks for each cashier, ordered first by `INITIAL_FLAG` and then by the `KPI_WEIGHTED` score:
        * `RED_FLAG_RANK`: Rank within their entire region (`REGIONALLEITER`).
        * `ORANGE_FLAG_RANK`: Rank within their specific store (`FILIALNR`).
    2.  **Applying Limits**: It uses the ranks to assign the final `FLAG`:
        * A **Red Flag (2)** is assigned if the cashier either had an absolute violation (`INITIAL_FLAG == 3`) OR they were a statistical red (`INITIAL_FLAG == 2`) AND their regional rank (`RED_FLAG_RANK`) is within the calculated limit for that region.
        * An **Orange Flag (1)** is assigned if the cashier is not given a red flag AND their store-level rank (`ORANGE_FLAG_RANK`) is within the `num_flags_per_store` limit.
* **Result**: The final DataFrame, containing only the cashiers who received a flag, ready for export.
