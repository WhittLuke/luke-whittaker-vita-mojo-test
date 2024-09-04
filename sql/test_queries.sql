/*
    Q1) How many total order where there?
*/

select 
    count(distinct id) as total_num_orders

from bundles
;


/*
    Q2) How many orders were from each channel (requestedFrom column is what showsthe channel)
*/

select 
    requested_from
    , count(distinct id) as total_num_orders

from bundles 

group by requested_from
;

/*
    Q3) How many items were sold for each hour of the day for each tenant?

    If I had enough time to finish the code part I would have added the link between 
    the orders and items tables this would be the query
*/

select 
    date_trunc('hour', orders.created_at) as order_date_hour
    , ext_tenant_uuid
    , count(order_items.item_id) as total_items_count

from orders 

left join order_items 
on orders.id = order_items.id 

group by 
    order_date_hour 
    , ext_tenant_uuid
;


/*
    Q4) What were the top 5 items sold for each tenant?
*/

with total_items_sold_per_tenant as (
    select 
        orders.ext_tenant_uuid
        , order_items.id as item_uuid
        , count(order_items.id) as total_items_count

    from orders 

    left join order_items 
    on orders.id = order_items.id 

    group by 
        order_date_hour 
        , ext_tenant_uuid
)

select 
    ext_tenant_uuid
    , item_uuid
    , total_items_count
    , rank() over (order by total_items_count desc) as rank

from total_items_sold_per_tenant
where rank <= 5
;

/*
    Q5) What were the items for each tenant that were sold more than 5 of?
*/
select 
    orders.ext_tenant_uuid
    , order_items.id as item_uuid
    , count(order_items.id) as total_items_count

from orders 

left join order_items 
on orders.id = order_items.id 

group by 
    order_date_hour 
    , ext_tenant_uuid

having count(order_items.id) > 5
;