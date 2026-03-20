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

-- Ensure client_id and considered_return_days exist; cast to int at render time
{% set client_id = client_id | required('client_id') | int %}
{% set considered_return_days = considered_return_days | required('considered_return_days') | int %}

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
        ) as transaction_id,
        receipt_line.ORIG_RECEIPT_STORE_ID AS RET_ORIG_RECEIPT_STORE_ID,
        receipt_line.ORIG_RECEIPT_REGISTER_ID AS RET_ORIG_RECEIPT_REGISTER_ID,
        receipt_line.ORIG_RECEIPT_DT AS RET_ORIG_RECEIPT_DT,
        receipt_line.ORIG_RECEIPT_ID AS RET_ORIG_RECEIPT_ID,
        receipt_line.ITEM_SID

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
        AND REGISTER_ID < 80  -- Exclude SCO
),

receipt_line_returns_aug AS (
    SELECT
        receipt_header.transaction_id,
        CONCAT(
            receipt_line_returns.ret_orig_receipt_store_id,
            receipt_line_returns.ret_orig_receipt_register_id,
            receipt_line_returns.ret_orig_receipt_dt,
            receipt_line_returns.ret_orig_receipt_id
        ) AS ret_orig_transaction_id,
        receipt_header.src_orig_store_nr,
        receipt_header.receipt_dt,
        receipt_header.receipt_tmsp,
        receipt_header.register_id,
        receipt_header.cashier_id,
        receipt_header.receipt_id,
        receipt_line_returns.item_sid
    FROM receipt_header
    INNER JOIN receipt_line_returns
        ON receipt_header.transaction_id = receipt_line_returns.transaction_id
),

original_receipt_line AS (
    SELECT
        ret_orig_transaction_id,
        item_sid,
        receipt_dt

    FROM (
        SELECT
            sales_piece_qty * sales_weight_qty AS qty,
            CONCAT(src_orig_store_nr, register_id, receipt_dt, receipt_id) AS ret_orig_transaction_id,
            item_sid,
            receipt_dt
        FROM
            UAPC.DATA_SHARED_LDL.P_LEW_CORE_AC_V_POS_RECEIPTLINE as receipt_line

        LEFT JOIN UAPC.DATA_SHARED_LDL.P_LEW_CORE_AC_V_LOCATION as stores
            ON receipt_line.LOC_SID = stores.LOC_SID

        WHERE
            receipt_line.CLIENT_ID = {{ client_id }}
            AND receipt_line.RECEIPT_DT BETWEEN DATEADD(day, -{{ considered_return_days }}, DATE('{{ receipt_monitoring_start_date }}')) AND DATE('{{ receipt_monitoring_end_date }}')
            {% if receipt_monitoring_store_list %}
            AND stores.SRC_ORIG_STORE_NR IN ({{ receipt_monitoring_store_list | as_sql_in_list }})
            {% else %}
            AND stores.SRC_ORIG_STORE_NR BETWEEN 100 AND 500
            {% endif %}
            AND receipt_line.TRANSACTION_TYPE_CD IN (101, 102)
            AND receipt_line.TURNOVER_RELEVANT_FG = 1
            AND receipt_line.RECEIPTLINE_VOIDED_BY IS NULL
    )

    GROUP BY
        ret_orig_transaction_id,
        item_sid,
        receipt_dt

    HAVING
        SUM(qty) IS NOT NULL
)

SELECT
    receipt_line_returns_aug.src_orig_store_nr,
    receipt_line_returns_aug.receipt_dt,
    receipt_line_returns_aug.receipt_tmsp,
    receipt_line_returns_aug.cashier_id,
    receipt_line_returns_aug.register_id,
    receipt_line_returns_aug.receipt_id

FROM receipt_line_returns_aug

LEFT JOIN original_receipt_line
    ON receipt_line_returns_aug.ret_orig_transaction_id = original_receipt_line.ret_orig_transaction_id
        AND receipt_line_returns_aug.item_sid = original_receipt_line.item_sid

WHERE
    -- No original receipt, if no receipt can be found, or it is older than 92 days
    original_receipt_line.receipt_dt IS NULL OR
    DATEDIFF(day, original_receipt_line.receipt_dt, receipt_line_returns_aug.receipt_dt) >= {{ considered_return_days }}
;
