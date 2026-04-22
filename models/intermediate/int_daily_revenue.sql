with orders as (

    select
        order_id,
        cast(order_time as date) as date,
        coalesce(estimated_order_value, 0) as estimated_order_value,
        coalesce(shipping_cost, 0) as shipping_cost
    from {{ ref('int_order_enriched') }}

)

select
    date,
    sum(estimated_order_value) as daily_product_revenue,
    sum(shipping_cost) as daily_shipping_revenue,
    sum(estimated_order_value + shipping_cost) as daily_revenue,
    count(distinct order_id) as daily_order_count
from orders
group by date
order by date