-- Ensure client_id exists; cast to int at render time
{% set client_id = client_id | required('client_id') | int %}

SELECT
    CASE
        WHEN WH_ID = 5 THEN 'WEI'
        WHEN WH_ID = 6 THEN 'SEZ'
        ELSE 'OTHER'
    END AS GES,
    CAST(ORIG_STORE_NR AS NUMBER(38, 0)) AS ORIG_STORE_NR,
    MOVE_STORE_DESC AS filiale,
    account_manager_name AS vertriebsleiter,
    sales_manager_name AS regionalleiter,
    sales_manager_name AS verkaufsleiter,
FROM
    UAPC.DATA_SHARED_LDL.P_LEW_MART_MSI_V_ORG_ALL_STORE_MERGE

WHERE 1=1
    AND CLIENT_ID = {{ client_id }}
    AND ASPECT_ID = 2   -- VGES
    AND DAT_CLOSING IS NULL
    AND ORIG_STORE_NR BETWEEN 100 AND 500
;
