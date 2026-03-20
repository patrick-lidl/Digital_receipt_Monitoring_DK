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

WITH receipt_header AS (
    SELECT
        TRIM(
            TO_VARCHAR(receipt_header.loc_sid)
            ||'_'||TO_VARCHAR(receipt_header.client_id)
            ||'_'||TO_VARCHAR(receipt_header.register_id)
            ||'_'||TO_CHAR(receipt_header.receipt_dt, 'YYYYMMDD')
            ||'_'||TO_CHAR(receipt_header.receipt_tmsp, 'HH24MISS')
            ||'_'||TO_VARCHAR(receipt_header.receipt_id)
        ) as transaction_id,
        stores.SRC_ORIG_STORE_NR,
        receipt_header.RECEIPT_DT,
        receipt_header.RECEIPT_TMSP,
        receipt_header.REGISTER_ID,
        receipt_header.CASHIER_ID,
        receipt_header.RECEIPT_ID,
        receipt_header.TOTAL_GROSS_VAL AS receipt_total_gross_val

    FROM UAPC.DATA_SHARED_LDL.P_LEW_CORE_AC_V_POS_RECEIPTHEADER as receipt_header

    LEFT JOIN UAPC.DATA_SHARED_LDL.P_LEW_CORE_AC_V_LOCATION as stores
        ON receipt_header.LOC_SID = stores.LOC_SID

    WHERE
        receipt_header.client_id = {{ client_id }}
        AND receipt_header.receipt_dt BETWEEN DATE('{{ receipt_monitoring_start_date }}') AND DATE('{{ receipt_monitoring_end_date }}')
        {% if receipt_monitoring_store_list %}
        AND stores.src_orig_store_nr IN ({{ receipt_monitoring_store_list | as_sql_in_list }})
        {% else %}
        AND stores.src_orig_store_nr BETWEEN 100 AND 500
        {% endif %}
        AND receipt_header.REGISTER_ID < 80  -- Exclude SCO
),

receipt_line_returns AS (
    SELECT
        TRIM(
            TO_VARCHAR(receipt_line.loc_sid)
            ||'_'||TO_VARCHAR(receipt_line.client_id)
            ||'_'||TO_VARCHAR(receipt_line.register_id)
            ||'_'||TO_CHAR(receipt_line.receipt_dt, 'YYYYMMDD')
            ||'_'||TO_CHAR(receipt_line.receipt_tmsp, 'HH24MISS')
            ||'_'||TO_VARCHAR(receipt_line.receipt_id)
        ) as transaction_id

    FROM UAPC.DATA_SHARED_LDL.P_LEW_CORE_AC_V_POS_RECEIPTLINE as receipt_line

    LEFT JOIN UAPC.DATA_SHARED_LDL.P_LEW_CORE_AC_V_LOCATION as stores
        ON receipt_line.LOC_SID = stores.LOC_SID

    WHERE
        receipt_line.client_id = {{ client_id }}
        AND receipt_line.receipt_dt BETWEEN DATE('{{ receipt_monitoring_start_date }}') AND DATE('{{ receipt_monitoring_end_date }}')
        {% if receipt_monitoring_store_list %}
        AND stores.src_orig_store_nr IN ({{ receipt_monitoring_store_list | as_sql_in_list }})
        {% else %}
        AND stores.src_orig_store_nr BETWEEN 100 AND 500
        {% endif %}
        AND receipt_line.RETURN_FG = 1
        AND receipt_line.TURNOVER_RELEVANT_FG = 1
        AND receipt_line.REGISTER_ID < 80  -- Exclude SCO
)

-- TODO: this could be saved as view and used in returns_without_original_receipt
SELECT
    receipt_header.src_orig_store_nr,
    receipt_header.receipt_dt,
    receipt_header.receipt_tmsp,
    receipt_header.register_id,
    receipt_header.cashier_id,
    receipt_header.receipt_id,
    receipt_header.receipt_total_gross_val  --used in m07
FROM receipt_header
INNER JOIN receipt_line_returns
    ON receipt_header.transaction_id = receipt_line_returns.transaction_id
;
