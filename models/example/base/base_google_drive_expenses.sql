select
    cast(date as date) as expense_date,
    try_to_decimal(regexp_replace(expense_amount, '[^0-9.\-]', ''), 18, 2) as expense_amount,
    lower(trim(expense_type)) as expense_type,
    trim(_file) as source_file,
    cast(_fivetran_synced as timestamp) as fivetran_synced_at,
    cast(_line as integer) as source_line_number,
    cast(_modified as timestamp) as source_modified_at
from {{ source('google_drive', 'expenses') }}
where date is not null