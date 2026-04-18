with sessions as (

    select
        session_id,
        client_id,
        session_time as session_at,
        os,
        ip
    from {{ ref('base_web_schema_sessions')}}

),

page_view_summary as (

    select
        session_id,
        1 as has_page_view,
        max(
            case
                when lower(page_name) like '%shop%' then 1
                else 0
            end
        ) as has_shop_page_view,
        count(*) as page_view_count
    from {{ ref('base_web_schema_page_views')}}
    group by session_id

),

item_view_summary as (

    select
        session_id,
        1 as has_item_view,
        max(case when coalesce(add_to_cart_quantity, 0) > 0 then 1 else 0 end) as has_add_to_cart,
        max(case when coalesce(remove_from_cart_quantity, 0) > 0 then 1 else 0 end) as has_remove_from_cart,
        count(*) as item_view_count,
        sum(coalesce(add_to_cart_quantity, 0)) as items_added_to_cart,
        sum(coalesce(remove_from_cart_quantity, 0)) as items_removed_from_cart
    from {{ ref('base_web_schema_item_views')}}
    group by session_id

),

orders_summary as (

    select
        session_id,
        1 as has_order,
        count(distinct order_id) as order_count,
        min(order_time) as first_order_at
    from {{ ref('base_web_schema_orders')}}
    group by session_id

),

item_ranked as (

    select
        session_id,
        item_name,
        item_view_time,
        price_per_unit,
        coalesce(add_to_cart_quantity, 0) as add_to_cart_quantity,
        coalesce(remove_from_cart_quantity, 0) as remove_from_cart_quantity,
        row_number() over (
            partition by session_id, item_name
            order by item_view_time desc
        ) as rn
    from {{ ref('base_web_schema_item_views')}}

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
        sum(coalesce(add_to_cart_quantity, 0)) - sum(coalesce(remove_from_cart_quantity, 0)) as net_quantity
    from {{ ref('base_web_schema_item_views')}}
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
    s.session_id,
    s.client_id,
    s.session_at,
    s.os,
    s.ip,

    coalesce(pv.has_page_view, 0) as has_page_view,
    coalesce(pv.has_shop_page_view, 0) as has_shop_page_view,
    coalesce(iv.has_item_view, 0) as has_item_view,
    coalesce(iv.has_add_to_cart, 0) as has_add_to_cart,
    coalesce(iv.has_remove_from_cart, 0) as has_remove_from_cart,
    coalesce(o.has_order, 0) as has_order,

    coalesce(pv.page_view_count, 0) as page_view_count,
    coalesce(iv.item_view_count, 0) as item_view_count,
    coalesce(iv.items_added_to_cart, 0) as items_added_to_cart,
    coalesce(iv.items_removed_from_cart, 0) as items_removed_from_cart,
    coalesce(o.order_count, 0) as order_count,

    o.first_order_at,
    coalesce(v.estimated_order_value, 0) as estimated_order_value

from sessions s
left join page_view_summary pv
    on s.session_id = pv.session_id
left join item_view_summary iv
    on s.session_id = iv.session_id
left join orders_summary o
    on s.session_id = o.session_id
left join session_value_proxy v
    on s.session_id = v.session_id