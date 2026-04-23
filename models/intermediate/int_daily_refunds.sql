with refunds_raw as (

    select
        order_id,
        returned_at_date,
        coalesce(is_refunded, 0) as is_refunded
    from {{ ref('base_google_drive_returns') }}

),

refunds as (

    select
        order_id,
        cast(max(returned_at_date) as date) as date,
        max(returned_at_date) as latest_returned_at_date,
        max(coalesce(is_refunded, 0)) as is_refunded
    from refunds_raw
    group by order_id

),

order_values as (

    select
        order_id,
        coalesce(estimated_order_value, 0) as estimated_order_value,
        coalesce(tax_rate, 0) as tax_rate
    from {{ ref('int_order_enriched') }}

),

refunds_enriched as (

    select
        r.date,
        r.order_id,
        
        case
            when r.latest_returned_at_date is not null then 1
            else 0
        end as is_returned,

        r.is_refunded,

        coalesce(o.estimated_order_value, 0) * (1 + coalesce(o.tax_rate, 0)) as estimated_order_gross_value

    from refunds r
    left join order_values o
        on r.order_id = o.order_id

)

select
    date,
    count(distinct case when is_returned = 1 then order_id end) as returned_order_count,
    count(distinct case when is_refunded = 1 then order_id end) as refunded_order_count,
    sum(case when is_returned = 1 then estimated_order_gross_value else 0 end) as estimated_return_value,
    sum(case when is_refunded = 1 then estimated_order_gross_value else 0 end) as estimated_refund_value
from refunds_enriched
group by date
order by date