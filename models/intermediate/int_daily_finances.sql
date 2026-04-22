with revenue as (

    select
        date,
        coalesce(daily_product_revenue, 0) as daily_product_revenue,
        coalesce(daily_shipping_revenue, 0) as daily_shipping_revenue,
        coalesce(daily_revenue, 0) as daily_revenue
    from {{ ref('int_daily_revenue') }}

),

costs as (

    select
        date,
        coalesce(other_expense_amount, 0) as other_expense_amount,
        coalesce(salary_cost_amount, 0) as salary_cost_amount,
        coalesce(total_cost_amount, 0) as total_cost_amount
    from {{ ref('int_daily_costs') }}

),

refunds as (

    select
        date,
        coalesce(returned_order_count, 0) as returned_order_count,
        coalesce(refunded_order_count, 0) as refunded_order_count,
        coalesce(estimated_return_value, 0) as estimated_return_value,
        coalesce(estimated_refund_value, 0) as estimated_refund_value
    from {{ ref('int_daily_refunds') }}

)

select
    r.date,
    r.daily_product_revenue,
    r.daily_shipping_revenue,
    r.daily_revenue,
    coalesce(c.other_expense_amount, 0) as other_expense_amount,
    coalesce(c.salary_cost_amount, 0) as salary_cost_amount,
    coalesce(c.total_cost_amount, 0) as total_cost_amount,
    coalesce(f.returned_order_count, 0) as returned_order_count,
    coalesce(f.refunded_order_count, 0) as refunded_order_count,
    coalesce(f.estimated_return_value, 0) as estimated_return_value,
    coalesce(f.estimated_refund_value, 0) as estimated_refund_value,
    r.daily_revenue
        - coalesce(c.total_cost_amount, 0)
        - coalesce(f.estimated_refund_value, 0) as profit_amount
from revenue r
left join costs c
    on r.date = c.date
left join refunds f
    on r.date = f.date
order by r.date