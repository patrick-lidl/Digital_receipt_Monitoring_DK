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
    pay.transaction_id
FROM (
    SELECT
        TRIM(
            TO_VARCHAR(pay.loc_sid)
            ||'_'||TO_VARCHAR(pay.client_id)
            ||'_'||TO_VARCHAR(pay.register_id)
            ||'_'||TO_CHAR(pay.receipt_dt, 'YYYYMMDD')
            ||'_'||TO_CHAR(pay.receipt_tmsp, 'HH24MISS')
            ||'_'||TO_VARCHAR(pay.receipt_id)
        ) as transaction_id,
        pay.tendertype_ejr_cd as payment_type
    FROM UAPC.DATA_SHARED_LDL.P_LEW_CORE_AC_V_POS_CTRL_PAYMENT as pay
    JOIN UAPC.DATA_SHARED_LDL.P_LEW_CORE_AC_V_LOCATION as stores
        ON pay.LOC_SID = stores.LOC_SID
    WHERE pay.client_id = {{ client_id }}
        AND pay.receipt_dt BETWEEN DATE('{{ receipt_monitoring_start_date }}') AND DATE('{{ receipt_monitoring_end_date }}')
        {% if receipt_monitoring_store_list %}
        AND stores.src_orig_store_nr IN ({{ receipt_monitoring_store_list | as_sql_in_list }})
        {% else %}
        AND stores.src_orig_store_nr BETWEEN 100 AND 500
        {% endif %}
        AND pay.REGISTER_ID < 80  -- Exclude SCO
) as pay
GROUP BY pay.transaction_id
HAVING COUNT(DISTINCT pay.payment_type) = 1
   AND MAX(pay.payment_type) = 11   -- cash only

;
