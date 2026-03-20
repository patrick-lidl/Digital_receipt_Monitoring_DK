/* ============================================================================
   Table: A_FILIALEN_STATUS
   Source: SSBI_LIDL_CO_INT_PRODUCTION.LEAD_KPI

   Description:
   This query retrieves dynamic daily store opening hours, replacing the legacy
   manually updated Excel flat file. It correctly captures holiday closures,
   special openings, and recent schedule changes.

   ⚠️ IMPORTANT HISTORICAL DATA LIMITATION ⚠️
   The timestamp columns (geoeffnet_von, geoeffnet_bis) are ONLY reliable for
   dates starting from November 6, 2024 onwards.

   For dates prior to 2024-11-06, the table correctly flags if a store was open
   (oeffnungstag_fg = 1), but the actual timestamp columns will contain NULLs.
   Do not use this table for deep historical reprocessing or backfilling prior
   to late 2024 without implementing fallback COALESCE logic.
============================================================================ */

-- Normalize optional params so theyre always defined
{% set receipt_monitoring_store_list = receipt_monitoring_store_list | default([], true) %}

-- Provide defaults, validate, and enforce presence
{% set receipt_monitoring_start_date =
     receipt_monitoring_start_date
     | default_previous_week_monday
     | ensure_date
     | required('receipt_monitoring_start_date') %}

{% set receipt_monitoring_end_date =
     receipt_monitoring_end_date
     | default_previous_week_sunday
     | ensure_date
     | required('receipt_monitoring_end_date') %}

-- Ensure client_id exists; cast to int at render time
{% set client_id = client_id | required('client_id') | int %}

SELECT
    filiale AS src_orig_store_nr,
    tag AS dat_formdate,
    oeffnungstag_fg,
    geoeffnet_von AS opening_timestamp,
    geoeffnet_bis AS closing_timestamp

FROM SSBI_LIDL_CO_INT_PRODUCTION.LEAD_KPI.A_FILIALEN_STATUS

WHERE CLIENT_ID = {{ client_id }}
    AND TAG BETWEEN DATE('{{ receipt_monitoring_start_date }}') AND DATE('{{ receipt_monitoring_end_date }}')
{% if receipt_monitoring_store_list %}
    AND FILIALE IN ({{ receipt_monitoring_store_list | as_sql_in_list }})
{% else %}
    AND FILIALE BETWEEN 100 AND 500
{% endif %}
