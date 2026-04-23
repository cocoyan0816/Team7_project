with orders as (

    select
        order_id,
        cast(order_time as date) as date,
        coalesce(estimated_order_value, 0) as estimated_order_value,
        coalesce(tax_rate, 0) as tax_rate
    from {{ ref('int_order_enriched') }}

)

select
    date,
    sum(estimated_order_value * (1 + tax_rate)) as daily_product_revenue,
    count(distinct order_id) as daily_order_count
from orders
group by date
order by date