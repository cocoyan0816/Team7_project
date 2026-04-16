select
    _file,
    _line,
    _modified,
    _fivetran_synced,
    case
        when lower(trim(is_refunded)) = 'yes' then 1
        when lower(trim(is_refunded)) = 'no' then 0
        else null
    end as is_refunded,
    cast(order_id as string) as order_id,
    cast(returned_at as date) as returned_at
from {{ source('google_drive', 'returns') }}