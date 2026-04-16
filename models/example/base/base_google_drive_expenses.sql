select
    _file,
    _line,
    _modified,
    _fivetran_synced,
    cast(date as date) as expense_date,
    try_to_decimal(regexp_replace(expense_amount, '[^0-9.\-]', ''), 18, 2) as expense_amount,
    expense_type
from {{ source('google_drive', 'expenses') }}