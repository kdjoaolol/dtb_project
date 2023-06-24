select

od.order_id,
od.product_id,
pr.supplier_id,
pr.category_id,
round(od.unit_price, 2) as unit_price,
od.quantity,
pr.product_name,
round(od.unit_price * od.quantity, 2) as total,
round((pr.unit_price * od.quantity) - total, 2) as discount

from {{source('sources', 'order_details')}} as od 
left join {{source('sources', 'products')}} as pr on od.product_id = pr.product_id