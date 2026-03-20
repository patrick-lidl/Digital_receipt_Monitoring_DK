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
    TRIM(
        TO_VARCHAR(receipt_line.loc_sid)
        ||'_'||TO_VARCHAR(receipt_line.client_id)
        ||'_'||TO_VARCHAR(receipt_line.register_id)
        ||'_'||TO_CHAR(receipt_line.receipt_dt, 'YYYYMMDD')
        ||'_'||TO_CHAR(receipt_line.receipt_tmsp, 'HH24MISS')
        ||'_'||TO_VARCHAR(receipt_line.receipt_id)
    ) as transaction_id,
    receipt_line.TRANSACTION_TYPE_CD,
    receipt_line.RECEIPTLINE_NR,
    receipt_line.RETURN_FG,
    -- 170, 172 are the original scans and 171, 173 are the voids, which is why
    -- 171, 173 have void_fg = 1 and the values are negative, while 170, 172 have RECEIPTLINE_VOIDED_BY
    CAST(receipt_line.RECEIPTLINE_VOIDED_BY > 0 AS INT) AS VOID_FG,
    TO_VARCHAR(receipt_line.EAN_BARCODE) AS EAN_BARCODE,   -- cast to string
    receipt_line.ITEM_SID

FROM UAPC.DATA_SHARED_LDL.P_LEW_CORE_AC_V_POS_RECEIPTLINE as receipt_line

LEFT JOIN UAPC.DATA_SHARED_LDL.P_LEW_CORE_AC_V_LOCATION as stores
    ON receipt_line.LOC_SID = stores.LOC_SID

WHERE
    1 = 1
    AND receipt_line.client_id = {{ client_id }}
    AND receipt_line.receipt_dt BETWEEN DATE('{{ receipt_monitoring_start_date }}') AND DATE('{{ receipt_monitoring_end_date }}')
    {% if receipt_monitoring_store_list %}
    AND stores.src_orig_store_nr IN ({{ receipt_monitoring_store_list | as_sql_in_list }})
    {% else %}
    AND stores.src_orig_store_nr BETWEEN 100 AND 500
    {% endif %}
    AND receipt_line.REGISTER_ID < 80  -- Exclude SCO
    AND receipt_line.TURNOVER_RELEVANT_FG = 1
;
