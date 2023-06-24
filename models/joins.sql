with prod as (

    select 

    c.category_name,
    s.company_name as suppliers,
    p.product_name,
    round(p.unit_price, 2) as unit_price,
    p.product_id

    from {{source('sources', 'products')}} as p
    left join {{source('sources', 'suppliers')}} as s on s.supplier_id = p.supplier_id
    left join {{source('sources', 'categories')}} as c on p.category_id = c.category_id
),

orddetail as (

    select 

    prd.*, 
    o.order_id,
    o.quantity,
    o.discount

    from {{ref('orderdetails')}} as o 
    left join prod as prd on prd.product_id = o.product_id
),

ordrs as (

    select 

    ord.order_date,
    ord.order_id,
    cs.company_name as customer,
    em.full_name as employee,
    em.age,
    em.lengthofservice

    from {{source('sources', 'orders')}} ord
    left join {{ref('customers')}} cs on cs.customer_id = ord.customer_id
    left join {{ref('employees')}} as em on ord.employee_id = em.employee_id
    left join {{source('sources', 'shippers')}} sh on ord.ship_via = sh.shipper_id

),

finaljoin as (

    select 

    od.*,
    ord.order_date,
    ord.customer, 
    ord.employee,
    ord.age,
    ord.lengthofservice

    from orddetail as od 
    inner join ordrs ord on od.order_id = ord.order_id

)

select * from finaljoin