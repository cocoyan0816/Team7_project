with all_dates as (

    select date from {{ ref('int_daily_revenue') }}
    union distinct
    select date from {{ ref('int_daily_refunds') }}
    union distinct
    select date from {{ ref('int_daily_costs') }}

),

daily_revenue as (

    select
        date,
        daily_product_revenue,
        daily_order_count
    from {{ ref('int_daily_revenue') }}

),

daily_refunds as (

    select
        date,
        returned_order_count,
        refunded_order_count,
        estimated_return_value,
        estimated_refund_value
    from {{ ref('int_daily_refunds') }}

),

daily_costs as (

    select
        date,
        other_expense_amount,
        salary_cost_amount,
        shipping_cost_amount,
        total_cost_amount
    from {{ ref('int_daily_costs') }}

)

select
    d.date,

    coalesce(r.daily_product_revenue, 0) as daily_product_revenue,
    coalesce(r.daily_order_count, 0) as daily_order_count,

    coalesce(f.returned_order_count, 0) as returned_order_count,
    coalesce(f.refunded_order_count, 0) as refunded_order_count,
    coalesce(f.estimated_return_value, 0) as estimated_return_value,
    coalesce(f.estimated_refund_value, 0) as estimated_refund_value,

    coalesce(c.other_expense_amount, 0) as other_expense_amount,
    coalesce(c.salary_cost_amount, 0) as salary_cost_amount,
    coalesce(c.shipping_cost_amount, 0) as shipping_cost_amount,
    coalesce(c.total_cost_amount, 0) as total_cost_amount,

    coalesce(r.daily_product_revenue, 0)
        - coalesce(f.estimated_refund_value, 0)
        - coalesce(c.total_cost_amount, 0) as daily_profit

from all_dates d
left join daily_revenue r
    on d.date = r.date
left join daily_refunds f
    on d.date = f.date
left join daily_costs c
    on d.date = c.date
order by d.date