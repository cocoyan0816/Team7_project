SELECT 
    "_fivetran_id" as _FIVETRAN_ID,
    IP,
    TO_TIMESTAMP(SESSION_AT) AS SESSION_TIME,
    OS,
    SESSION_ID,
    CAST(CLIENT_ID AS STRING) AS CLIENT_ID,
    "_fivetran_deleted" as _FIVETRAN_DELETE,
    "_fivetran_synced" AS _fivetran_synced_TS
FROM {{ source('web_schema', 'sessions') }}