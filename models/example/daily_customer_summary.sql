{{
    config(
        alias="daily_customer_summary",
        materialized="incremental",
        unique_key=["dw_customer_id","summarydate"],
        incremental_strategy="merge",
    )
}}
with DCS as
(
select 
a.summarydate,
a.dw_customer_id,
MAX(a.order_count) as order_count,
MAX(a.order_apd) as order_apd,
MAX(a.ordered_amt) as ordered_amt,
MAX(a.order_cost_amount) as order_cost_amount,
MAX(a.order_mrp_amount) as order_mrp_amount,
MAX(a.products_ordered_qty) as products_ordered_qty,
MAX(products_items_qty) as products_items_qty,
MAX(cancelled_order_count) as cancelled_order_count,
MAX(cancelled_order_amount) as cancelled_order_amount,
MAX(cancelled_order_apd) as cancelled_order_apd,
MAX(shipped_order_count) as shipped_order_count,
MAX(shipped_order_amount) as shipped_order_amount,
MAX(shipped_order_apd) as shipped_order_apd,
MAX(payment_apd) as payment_apd,
MAX(payment_amount) as payment_amount,
MAX(new_customer_apd) as new_customer_apd,
MAX(new_customer_paid_apd) as new_customer_paid_apd,
MAX(e1.etl_batch_no) as etl_batch_no,
MAX(e1.etl_batch_date) as etl_batch_date
FROM
(select o.orderDate as summarydate,
c.dw_customer_id,
COUNT(DISTINCT od.src_orderNumber) as order_count,
1 as order_apd,
SUM(od.quantityOrdered * od.priceEach) as ordered_amt,
SUM(od.quantityOrdered * p.buyPrice) as order_cost_amount,
SUM(od.quantityOrdered * p.MSRP) as order_mrp_amount,
COUNT(od.src_productCode) as products_ordered_qty,
COUNT(DISTINCT p.ProductLine) as products_items_qty,
0 as cancelled_order_count,
0 as cancelled_order_amount,
0 as cancelled_order_apd,
0 as shipped_order_count,
0 as shipped_order_amount,
0 as shipped_order_apd,
0 as payment_apd,
0 as payment_amount,
0 as new_customer_apd,
0 as new_customer_paid_apd
FROM
{{ ref('customers') }} c INNER JOIN {{ ref('orders') }} o
ON c.dw_customer_id = o.dw_customer_id
INNER JOIN {{ ref('orderdetails') }} od 
ON o.dw_order_id = od.dw_order_id
INNER JOIN {{ ref('products') }} p 
ON od.dw_product_id = p.dw_product_id
WHERE o.orderDate >= (select max(etl_batch_date) FROM etl_metadata.batch_control)
GROUP BY 1,2
UNION ALL
select o.cancelledDate,
c.dw_customer_id,
0 as order_count,
0 as order_apd,
0 as order_amt,
0 as order_cost_amount,
0 as order_mrp_amount,
0 as products_ordered_qty,
0 as products_items_qty ,
COUNT(DISTINCT o.src_orderNumber) as cancelled_order_count,
SUM(od.quantityOrdered * od.priceEach) as cancelled_order_amount,
1 as cancelled_order_apd,
0 as shipped_order_count,	
0 as shipped_order_amount,	
0 as shipped_order_apd,	
0 as payment_apd,	
0 as payment_amount,	
0 as new_customer_apd,
0 as new_customer_paid_apd
 FROM 
{{ ref('customers') }} c INNER JOIN {{ ref('orders') }} o 
ON c.dw_customer_id = o.dw_customer_id
INNER JOIN {{ ref('orderdetails') }} od ON
o.dw_order_id = od.dw_order_id
WHERE o.cancelledDate >= (select max(etl_batch_date) FROM etl_metadata.batch_control)
GROUP BY 1,2
UNION ALL
select o.shippedDate,o.dw_customer_id,
0 as order_count,	
0 as order_apd,	
0 as order_amt,	
0 as order_cost_amount,	
0 as order_mrp_amount,	
0 as products_ordered_qty,	
0 as products_items_qty,	
0 as cancelled_order_count,	
0 as cancelled_order_amount,	
0 as cancelled_order_apd,	
COUNT(DISTINCT o.src_orderNumber) as shipped_order_count,
SUM(od.quantityOrdered * od.priceEach) as shipped_order_amount,
1 as shipped_order_apd,
0 as payment_apd,	
0 as payment_amount,	
0 as new_customer_apd,
0 as new_customer_paid_apd
FROM {{ ref('orders') }} o
INNER JOIN {{ ref('orderdetails') }} od ON
o.dw_order_id = od.dw_order_id
WHERE o.shippedDate >= (select max(etl_batch_date) FROM etl_metadata.batch_control)
GROUP BY 1,2
UNION ALL
select paymentDate,
dw_customer_id,
 0 as order_count,	
0 as order_apd,	
0 as order_amt,	
0 as order_cost_amount,	
0 as order_mrp_amount,	
0 as products_ordered_qty,	
0 as products_items_qty,	
0 as cancelled_order_count,	
0 as cancelled_order_amount,	
0 as cancelled_order_apd,	
0 as shipped_order_count,	
0 as shipped_order_amount,	
0 as shipped_order_apd,	
1 as payment_apd,
SUM(amount) as payment_amt,
0 as new_customer_apd,
0 as new_customer_paid_apd
FROM {{ ref('payments') }}
WHERE paymentDate >= (select max(etl_batch_date) FROM etl_metadata.batch_control)
GROUP BY 1,2
UNION ALL
select DATE(src_create_timestamp),
dw_customer_id,
0 as order_count,
0 as order_apd,
0 as order_amt,
0 as order_cost_amount,
0 as order_mrp_amount,
0 as products_ordered_qty,
0 as products_items_qty,
0 as cancelled_order_count,
0 as cancelled_order_amount,
0 as cancelled_order_apd,
0 as shipped_order_count,
0 as shipped_order_amount,
0 as shipped_order_apd,
0 as payment_apd,
0 as payment_amount,
1 as new_customer_apd,
0 as new_customer_paid_apd
FROM {{ ref('customers') }}
WHERE DATE(src_create_timestamp)>=(select max(etl_batch_date) FROM etl_metadata.batch_control)
GROUP BY 1,2 ) a
CROSS JOIN etl_metadata.batch_control e1
GROUP BY 1,2
),
t1 as (
    select dw_customer_id,MIN(summarydate) as min_orderdate
    FROM DCS
    WHERE order_apd = 1 
    GROUP BY dw_customer_id    
)
select 
summarydate,
DCS.dw_customer_id,
order_count,
order_apd,
ordered_amt,
order_cost_amount,
order_mrp_amount,
products_ordered_qty,
products_items_qty,
cancelled_order_count,
cancelled_order_amount,
cancelled_order_apd,
shipped_order_count,
shipped_order_amount,
shipped_order_apd,
payment_apd,
payment_amount,
new_customer_apd,
CASE 
WHEN DCS.summarydate = t1.min_orderdate
THEN 1 ELSE 0
END as new_customer_paid_apd,
current_timestamp as dw_create_timestamp,
etl_batch_no,
etl_batch_date
FROM DCS LEFT JOIN t1
ON DCS.dw_customer_id = t1.dw_customer_id
