with session_events as (

    select * from {{ ref('int_session_events') }}

)

select
    session_id,
    1 as has_session, 
    
    coalesce(has_shop_page_view, 0) as step_view_shop_page,
    coalesce(has_item_view, 0) as step_view_item,
    coalesce(has_add_to_cart, 0) as step_add_to_cart,
    coalesce(has_order, 0) as step_place_order

from session_events