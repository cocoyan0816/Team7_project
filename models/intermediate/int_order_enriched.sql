with order_rank as (

    select
        *,
        row_number() over (
            partition by order_id
            order by order_time desc
        ) as row_n
    from {{ ref('base_web_schema_orders') }}

),

orders as (

    select
        order_id,
        session_id,
        order_time,
        client_name,
        state as shipping_state,
        payment_method,
        shipping_cost,
        tax_rate
    from order_rank
    where row_n = 1

),

session_rank as (

    select
        *,
        row_number() over (
            partition by session_id
            order by session_time desc
        ) as row_n
    from {{ ref('base_web_schema_sessions') }}

),

sessions as (

    select
        session_id,
        client_id
    from session_rank
    where row_n = 1

),

returns as (

    select
        order_id,
        is_refunded,
        returned_at_date
    from {{ ref('base_google_drive_returns') }}

),

item_views as (

    select
        session_id,
        item_name,
        item_view_time,
        price_per_unit,
        coalesce(add_to_cart_quantity, 0) as add_to_cart_quantity,
        coalesce(remove_from_cart_quantity, 0) as remove_from_cart_quantity
    from {{ ref('base_web_schema_item_views') }}

),

item_ranked as (

    select
        session_id,
        item_name,
        item_view_time,
        price_per_unit,
        row_number() over (
            partition by session_id, item_name
            order by item_view_time desc
        ) as rn
    from item_views

),

latest_price as (

    select
        session_id,
        item_name,
        price_per_unit as latest_price_per_unit
    from item_ranked
    where rn = 1

),

item_net_qty as (

    select
        session_id,
        item_name,
        sum(add_to_cart_quantity) - sum(remove_from_cart_quantity) as net_quantity
    from item_views
    group by session_id, item_name

),

session_value_proxy as (

    select
        q.session_id,
        sum(q.net_quantity * p.latest_price_per_unit) as estimated_order_value
    from item_net_qty q
    join latest_price p
        on q.session_id = p.session_id
       and q.item_name = p.item_name
    where q.net_quantity > 0
    group by q.session_id

)

select
    o.order_id,
    o.session_id,
    s.client_id,
    o.order_time,
    o.client_name,
    o.shipping_state,
    o.payment_method,
    o.shipping_cost,
    o.tax_rate,
    coalesce(v.estimated_order_value, 0) as estimated_order_value,

    case
        when r.returned_at_date is not null then 1
        else 0
    end as is_returned,

    coalesce(r.is_refunded, 0) as is_refunded,
    r.returned_at_date,

    case
        when coalesce(r.is_refunded, 0) = 1
            then - coalesce(o.shipping_cost, 0)
        else
            coalesce(v.estimated_order_value, 0) * (1 + coalesce(o.tax_rate, 0))
            - coalesce(o.shipping_cost, 0)
    end as profit

from orders o
left join sessions s
    on o.session_id = s.session_id
left join returns r
    on o.order_id = r.order_id
left join session_value_proxy v
    on o.session_id = v.session_id