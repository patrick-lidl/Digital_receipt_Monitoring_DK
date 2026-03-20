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
        TO_VARCHAR(subtotal_line.loc_sid)
        ||'_'||TO_VARCHAR(subtotal_line.client_id)
        ||'_'||TO_VARCHAR(subtotal_line.register_id)
        ||'_'||TO_CHAR(subtotal_line.receipt_dt, 'YYYYMMDD')
        ||'_'||TO_CHAR(subtotal_line.receipt_tmsp, 'HH24MISS')
        ||'_'||TO_VARCHAR(subtotal_line.receipt_id)
    ) as transaction_id,
    subtotal_line.RECEIPTLINE_NR,
    stores.SRC_ORIG_STORE_NR,
    subtotal_line.RECEIPT_DT,
    subtotal_line.RECEIPT_TMSP,
    subtotal_line.REGISTER_ID,
    subtotal_line.CASHIER_ID,
    subtotal_line.RECEIPT_ID

FROM UAPC.DATA_SHARED_LDL.P_LEW_CORE_AC_V_POS_CTRL_SUBTOTAL as subtotal_line

LEFT JOIN UAPC.DATA_SHARED_LDL.P_LEW_CORE_AC_V_LOCATION as stores
    ON subtotal_line.LOC_SID = stores.LOC_SID

WHERE
    subtotal_line.client_id = {{ client_id }} AND
    subtotal_line.receipt_dt between DATE('{{ receipt_monitoring_start_date }}') AND DATE('{{ receipt_monitoring_end_date }}')
    {% if receipt_monitoring_store_list %}
    AND stores.src_orig_store_nr IN ({{ receipt_monitoring_store_list | as_sql_in_list }})
    {% else %}
    AND stores.src_orig_store_nr BETWEEN 100 AND 500
    {% endif %}
    AND subtotal_line.REGISTER_ID < 80  -- Exclude SCO
;
