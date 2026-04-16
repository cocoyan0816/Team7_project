select
    _file,
    _line,
    _modified,
    _fivetran_synced,
    is_refunded,
    cast(order_id as string) as order_id,
    returned_at
from {{ source('google_drive', 'returns') }}