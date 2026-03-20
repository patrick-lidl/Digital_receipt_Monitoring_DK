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
        TO_VARCHAR(receipt_header.loc_sid)
        ||'_'||TO_VARCHAR(receipt_header.client_id)
        ||'_'||TO_VARCHAR(receipt_header.register_id)
        ||'_'||TO_CHAR(receipt_header.receipt_dt, 'YYYYMMDD')
        ||'_'||TO_CHAR(receipt_header.receipt_tmsp, 'HH24MISS')
        ||'_'||TO_VARCHAR(receipt_header.receipt_id)
    ) as TRANSACTION_ID,
    receipt_header.TRANSACTION_TYPE_CD,
    receipt_header.CNT_RECEIPTLINES_SALES,
    receipt_header.CNT_RECEIPTLINES_VOIDS,
    receipt_header.TURNOVER_RELEVANT_FG,
    -- KPI index cols
    stores.SRC_ORIG_STORE_NR,
    receipt_header.RECEIPT_DT,
    receipt_header.RECEIPT_TMSP,
    receipt_header.REGISTER_ID,
    receipt_header.CASHIER_ID,
    receipt_header.RECEIPT_ID

FROM UAPC.DATA_SHARED_LDL.P_LEW_CORE_AC_V_POS_RECEIPTHEADER as receipt_header

LEFT JOIN UAPC.DATA_SHARED_LDL.P_LEW_CORE_AC_V_LOCATION as stores
    ON receipt_header.LOC_SID = stores.LOC_SID

LEFT JOIN UAPC.DATA_SHARED_LDL.P_LEW_BIL_AC_V_STORE_TO_WAREHOUSE_LASTKNOWN as store_wh
    ON receipt_header.LOC_SID = store_wh.LOC_SID_STORE

WHERE
    receipt_header.client_id = {{ client_id }}
    AND receipt_header.receipt_dt BETWEEN DATE('{{ receipt_monitoring_start_date }}') AND DATE('{{ receipt_monitoring_end_date }}')
{% if receipt_monitoring_store_list %}
    AND stores.src_orig_store_nr IN ({{ receipt_monitoring_store_list | as_sql_in_list }})
{% else %}
    AND stores.src_orig_store_nr BETWEEN 100 AND 500
{% endif %}
;
