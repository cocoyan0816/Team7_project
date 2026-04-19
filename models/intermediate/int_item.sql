with item_base as (
    SELECT *
    FROM {{ ref('base_web_schema_item_views')}}
)

SELECT
    item_name,
    max(price_per_unit) as price_per_unit,
    min(item_view_time) as first_seen_at,
    max(item_view_time) as latest_seen_at,
    count(*) as total_item_views,
    sum(add_to_cart_quantity) as total_add_to_cart_quantity,
    sum(remove_from_cart_quantity) as total_remove_from_cart_quantity
from item_base
group by item_name