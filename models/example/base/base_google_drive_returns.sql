select
    _file,
    _line,
    _modified as modified_ts,
    _fivetran_synced as _fivetran_synced_ts,
    is_refunded,
    cast(order_id as string) as order_id,
    returned_at
from {{ source('google_drive', 'returns') }}