{{
    config(
        alias="monthly_product_summary",
        materialized="incremental",
        unique_key=["dw_product_id","start_of_the_month_date"],
        incremental_strategy="merge",
    )
}}
SELECT DATE_TRUNC('MONTH',orderDate) start_of_the_month_date,
dw_product_id,
SUM(customer_apd) as customer_apd,
CASE 
WHEN SUM(customer_apd) > 0 
THEN 1 ELSE 0 
END as customer_apm,
SUM(product_order_amount) as product_order_amount,
SUM(product_cost_amount) as product_cost_amount,
SUM(product_MSRP_amount) as product_msrp_amount,
SUM(cancelled_product_qty) as cancelled_product_qty,
SUM(cancelled_order_amount) as cancelled_order_amount,
SUM(cancelled_cost_amount) as cancelled_cost_amount,
SUM(cancelled_msrp_amount) as cancelled_mrp_amount,
SUM(cancelled_order_apd) as cancelled_order_apd,
CASE
WHEN SUM(cancelled_order_apd) > 0
THEN 1 ELSE 0
END as customer_order_apm,
current_timestamp as dw_create_timestamp,
MAX(etl_batch_no) as etl_batch_no, 
MAX(etl_batch_date) as etl_batch_date
FROM {{ ref('daily_product_summary') }} p1
GROUP BY 1,2
ORDER BY 1,2