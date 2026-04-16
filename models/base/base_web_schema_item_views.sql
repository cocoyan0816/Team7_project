SELECT 
    "_fivetran_id" as _FIVETRAN_ID,
    cast(PRICE_PER_UNIT as float) as PRICE_PER_UNIT,
    DATE_TRUNC('SECOND', ITEM_VIEW_AT) AS ITEM_VIEW_TIME,
    ITEM_NAME,
    SESSION_ID,
    ADD_TO_CART_QUANTITY,
    REMOVE_FROM_CART_QUANTITY,
    "_fivetran_deleted" as _FIVETRAN_DELETE,
    DATE_TRUNC('SECOND',"_fivetran_synced") AS _fivetran_synced_TS
FROM {{ source('web_schema', 'item_views') }}
