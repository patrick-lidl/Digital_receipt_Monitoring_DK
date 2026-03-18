# Mendix Frontend: Digital Receipt Monitoring App

This document provides a comprehensive overview of the Mendix-based iPad application that serves as the user interface for the Digital Receipt Monitoring pipeline.

## 1. Overview

The Mendix app is a web-based, iPad-optimized frontend designed for Regional Managers (RL) and Sales Managers (VT) to review, investigate, and act upon the anomalies flagged by the backend PySpark pipeline.

Its core purpose is to transform the raw data output into an intuitive, actionable workflow, and to create a feedback loop that allows the business to provide context on the flagged anomalies, thereby enabling continuous improvement of the detection models.

## 2. User Access & Roles

User authentication and authorization are managed via the company's standard role system (SIAM) with Single Sign-On (SSO). The view within the app is tailored to the user's position in the sales hierarchy:

* **Regional Manager (RL)**: Sees a view focused on the stores within their specific region. Their primary task is to complete the weekly audits for their stores.
* **Sales Manager (VT)**: Sees a higher-level, aggregated view of all the RLs and stores under their management. They can monitor the audit completion status across their entire area of responsibility.

## 3. Core Workflow: The User Journey

The user journey is designed as a three-step drill-down process, from a high-level overview to a specific, actionable task.

### Step 1: The Home Screen (VT/RL Overview)

This is the main landing page. A Sales Manager (VT) sees an overview of all their Regional Managers (RLs) on the left and a grid of the corresponding stores on the right.

* **Task Grid**: The main grid shows the audit status for each store for the last 12 calendar weeks (`KW`).
    * **Green Check (✅)**: Indicates the audit for that store and week has been completed by the RL.
    * **Red Cross (❌)**: Indicates an open, pending audit that requires action.
* **Navigation**: Clicking on any icon in the grid takes the user to the Store Audit Overview for that specific store and week.

### Step 2: The Store Audit Overview

This screen provides a detailed breakdown of all flagged anomalies for a single store in a given week, presented as a KPI vs. Cashier (BED) grid.

* **Red Flags (Magnifying Glass 🔍)**: These represent high-priority anomalies (Grade 2) that the RL is **required** to investigate.
* **Orange Flags (Binoculars 🔭)**: These are lower-priority anomalies (Grade 1) provided for additional context and awareness. They are not mandatory to investigate.
* **Progress Bar**: A visual indicator at the top of the screen tracks the completion progress of the mandatory (red flag) tasks for that week.
* **Historical Context**: A cashier's ID is highlighted in red if they were also flagged for any anomaly in the preceding week, allowing managers to quickly spot recurring issues.
* **Navigation**: Clicking on a magnifying glass or binocular icon drills down to the final Action Screen for that specific KPI and cashier.

### Step 3: The Action Screen (Task Completion & Feedback)

This is the final screen where the manager investigates a specific flag and provides feedback.

* **Context Section**: The top of the screen provides a comprehensive explanation of the flagged KPI, including:
    * **Was wird gemessen?** (What is being measured?)
    * **Wie sind die Grenzwerte definiert?** (How are the thresholds defined?)
    * **Mögliche Erkenntnisse** (Potential root causes)
    * **Was kann ich tun?** (Recommended actions)
* **Evidence List**: A table lists all the specific receipt numbers (`Receipt.nr`) that contributed to this anomaly, along with their date and time. *Future Enhancement: A planned feature is to render the full digital receipt directly in this view.*
* **Feedback & Action Sidebar**: On the right, the manager completes the task:
    1.  **Commentary**: A text box allows for detailed notes about the investigation.
    2.  **Incident Type**: A mandatory dropdown menu is used to categorize the outcome of the investigation. These categories (e.g., `Plausibel`, `Nachschulung`, `Manipulation`) are managed by key users within the app itself.
    3.  **Save**: Clicking "Save" submits the feedback, logs the action in the timeline, and marks this specific task as complete, updating the progress bar on the previous screen.

## 4. Key Features

### Dashboard
A dedicated dashboard provides a historical view of the feedback data, allowing VTs and RLs to analyze trends over time. Users can filter by RL, store, and cashier to see the types of incidents that have been logged across different weeks.

### Notification System
An automated email notification system keeps users engaged:
* An email is sent to RLs when new weekly data is available.
* A reminder email is sent for any open tasks that have not been completed.
* If tasks remain open for a specified number of weeks, the user's VT is automatically notified via email to ensure accountability.

### Data Management & The Feedback Loop
* **Data Retention**: All anomaly data sent from the backend pipeline is automatically deleted after 12 weeks to comply with data protection standards.
* **Closing the Loop**: The structured feedback data (incident types, comments) is the most valuable output of the app. This data can be exported and provided to the data science team. It serves two critical purposes:
    1.  **Project Evaluation**: It provides the ground truth needed to measure the accuracy and impact of the anomaly detection models.
    2.  **Model Improvement**: By analyzing which flags are consistently marked as `Plausibel` (False Positives), the backend models can be retrained and improved, creating a virtuous cycle.

## 5. Data Interface

The backend PySpark pipeline communicates with the Mendix application via a REST API managed by the IT department.

* **Endpoint**: The `ExportReceiptMonitoringJson` task in the pipeline is responsible for formatting the final flagged cashier data into the required JSON structure.
* **Payload**: This JSON payload is then sent via a `POST` request to the Mendix API, which ingests the data and stores it in its internal PostgreSQL database.
