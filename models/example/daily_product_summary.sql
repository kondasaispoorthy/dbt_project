{{
    config(
        alias="daily_product_summary",
        materialized="incremental",
        unique_key=["dw_product_id","orderDate"],
        incremental_strategy="merge",
    )
}}
select orderDate,
dw_product_id,
MAX(customer_apd) as customer_apd,
MAX(product_order_amount) as product_order_amount,
MAX(product_cost_amount) as product_cost_amount,
MAX(product_MSRP_amount) as product_MSRP_amount,
SUM(cancelled_product_qty) as cancelled_product_qty,
MAX(cancelled_order_amount) as cancelled_order_amount,
MAX(cancelled_cost_amount) as cancelled_cost_amount,
MAX(cancelled_MSRP_amount) as cancelled_MSRP_amount,
MAX(cancelled_order_apd) as cancelled_order_apd,
MAX(e1.etl_batch_no) as etl_batch_no,
MAX(e1.etl_batch_date) as etl_batch_date,
current_timestamp as dw_create_timestamp
FROM (
select o.orderDate,p.dw_product_id,
COUNT(DISTINCT dw_customer_id) as customer_apd,
SUM(od.quantityOrdered * od.priceEach) as product_order_amount,
SUM(od.quantityOrdered * p.buyPrice) as product_cost_amount,
SUM(od.quantityOrdered * p.MSRP) as product_MSRP_amount,
0 as cancelled_product_qty,
0 as cancelled_order_amount,
0 as cancelled_cost_amount,
0 as cancelled_MSRP_amount,
0 as cancelled_order_apd
from {{ ref('orders') }} o INNER JOIN {{ ref('orderdetails') }} od ON 
o.dw_order_id = od.dw_order_id
INNER JOIN {{ ref('products') }} p ON
od.dw_product_id = p.dw_product_id
WHERE o.orderDate >=  (select max(etl_batch_date) from etl_metadata.batch_control )
GROUP BY 1,2
UNION ALL
select o.cancelledDate,
p.dw_product_id,
0 as customer_apd,
0 as product_order_amount,
0 as product_cost_amount,
0 as product_MSRP_amount,
COUNT(p.dw_product_id) as cancelled_product_qty,
SUM(od.quantityOrdered * od.priceEach) as cancelled_order_amount,
SUM(od.quantityOrdered * p.buyPrice) as cancelled_cost_amount,
SUM(od.quantityOrdered * p.MSRP) as cancelled_MSRP_amount,
1 as cancelled_order_apd
FROM 
{{ ref('orders') }} o INNER JOIN {{ ref('orderdetails') }} od 
ON o.dw_order_id = od.dw_order_id
INNER JOIN {{ ref('products') }} p ON
od.dw_product_id = p.dw_product_id
WHERE o.cancelledDate >=  (select max(etl_batch_date) from etl_metadata.batch_control )
GROUP BY 1,2) x
CROSS JOIN etl_metadata.batch_control e1
GROUP BY 1,2