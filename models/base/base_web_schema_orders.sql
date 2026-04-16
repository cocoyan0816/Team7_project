SELECT
    "_fivetran_id" as _FIVETRAN_ID,
    ORDER_ID,
    TAX_RATE,
    SHIPPING_ADDRESS,
    TRY_TO_NUMBER(REPLACE(SHIPPING_COST, 'USD ', '')) AS SHIPPING_COST,
    PAYMENT_METHOD,
    PAYMENT_INFO,
    PHONE AS PHONE_NUMBER,
    ORDER_AT AS ORDER_TIME,
    STATE,
    CLIENT_NAME,
    SESSION_ID,
    "_fivetran_deleted" as _FIVETRAN_DELETE,
    DATE_TRUNC('SECOND',"_fivetran_synced") AS _fivetran_synced_TS
FROM {{ source('web_schema', 'orders') }}