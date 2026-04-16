SELECT
    "_fivetran_id" as _FIVETRAN_ID,
    PAGE_NAME,
    DATE_TRUNC('SECOND', VIEW_AT) AS PAGE_VIEW_TIME,
    SESSION_ID,
    "_fivetran_deleted" as _FIVETRAN_DELETE,
    DATE_TRUNC('SECOND',"_fivetran_synced") AS _fivetran_synced_TS
FROM {{ source('web_schema', 'page_views') }}