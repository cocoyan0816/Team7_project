with sessions as (

    select
        client_id,
        session_id,
        session_time
    from (
        select
            client_id,
            session_id,
            session_time,
            row_number() over (
                partition by session_id
                order by session_time asc
            ) as rn
        from {{ ref('base_web_schema_sessions') }}
        where client_id is not null
    )
    where rn = 1

),

orders as (

    select
        order_id,
        session_id,
        client_name,
        order_time,
        shipping_state
    from (
        select
            order_id,
            session_id,
            client_name,
            order_time,
            state as shipping_state,
            row_number() over (
                partition by order_id
                order by order_time asc
            ) as rn
        from {{ ref('base_web_schema_orders') }}
    )
    where rn = 1

),

page_views as (

    select
        session_id
    from {{ ref('base_web_schema_page_views')}}

),

item_views as (

    select
        session_id,
        item_name,
        item_view_time,
        price_per_unit,
        coalesce(add_to_cart_quantity, 0) as add_to_cart_quantity,
        coalesce(remove_from_cart_quantity, 0) as remove_from_cart_quantity
    from {{ ref('base_web_schema_item_views')}}

),

session_summary as (

    select
        client_id,
        min(session_time) as first_session_at,
        max(session_time) as latest_session_at,
        count(distinct session_id) as total_sessions
    from sessions
    group by client_id

),

page_view_summary as (

    select
        s.client_id,
        count(*) as total_page_views
    from page_views pv
    join sessions s
        on pv.session_id = s.session_id
    group by s.client_id

),

item_summary as (

    select
        s.client_id,
        count(*) as total_item_views,
        sum(iv.add_to_cart_quantity) as total_add_to_cart_quantity,
        sum(iv.remove_from_cart_quantity) as total_remove_from_cart_quantity
    from item_views iv
    join sessions s
        on iv.session_id = s.session_id
    group by s.client_id

),

order_with_client as (

    select
        s.client_id,
        o.order_id,
        o.client_name,
        o.order_time,
        o.shipping_state,
        o.session_id
    from orders o
    join sessions s
        on o.session_id = s.session_id

),

order_summary as (

    select
        client_id,
        min(order_time) as first_order_at,
        max(order_time) as latest_order_at,
        count(distinct order_id) as total_orders
    from order_with_client
    group by client_id

),

client_name_final as (

    select
        client_id,
        client_name
    from (
        select
            client_id,
            client_name,
            row_number() over (
                partition by client_id
                order by order_time desc
            ) as rn
        from order_with_client
        where client_name is not null
    )
    where rn = 1

),

shipping_state_final as (

    select
        client_id,
        shipping_state
    from (
        select
            client_id,
            shipping_state,
            row_number() over (
                partition by client_id
                order by order_time desc
            ) as rn
        from order_with_client
        where shipping_state is not null
    )
    where rn = 1

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
    group by q.session_id

),

client_value_summary as (

    select
        owc.client_id,
        sum(svp.estimated_order_value) as estimated_total_purchase_value
    from order_with_client owc
    left join session_value_proxy svp
        on owc.session_id = svp.session_id
    group by owc.client_id

)

select
    s.client_id,
    n.client_name,
    st.shipping_state,
    s.first_session_at,
    s.latest_session_at,
    o.first_order_at,
    o.latest_order_at,
    s.total_sessions,
    coalesce(o.total_orders, 0) as total_orders,
    coalesce(pv.total_page_views, 0) as total_page_views,
    coalesce(iv.total_item_views, 0) as total_item_views,
    coalesce(iv.total_add_to_cart_quantity, 0) as total_add_to_cart_quantity,
    coalesce(iv.total_remove_from_cart_quantity, 0) as total_remove_from_cart_quantity,
    coalesce(cv.estimated_total_purchase_value, 0) as estimated_total_purchase_value
from session_summary s
left join order_summary o
    on s.client_id = o.client_id
left join client_name_final n
    on s.client_id = n.client_id
left join shipping_state_final st
    on s.client_id = st.client_id
left join page_view_summary pv
    on s.client_id = pv.client_id
left join item_summary iv
    on s.client_id = iv.client_id
left join client_value_summary cv
    on s.client_id = cv.client_id