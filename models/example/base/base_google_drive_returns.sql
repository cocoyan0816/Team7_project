select
    case
        when lower(trim(is_refunded)) in ('true', 't', 'yes', 'y', '1') then true
        when lower(trim(is_refunded)) in ('false', 'f', 'no', 'n', '0') then false
        else null
    end as is_refunded,
    trim(order_id) as order_id,
    cast(returned_at as date) as returned_at,
    trim(_file) as source_file,
    cast(_fivetran_synced as timestamp_ntz) as fivetran_synced_at,
    cast(_line as integer) as source_line_number,
    cast(_modified as timestamp_ntz) as source_modified_at
from {{ source('google_drive', 'returns') }}
where order_id is not null