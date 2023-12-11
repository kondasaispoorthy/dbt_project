{{
    config(
        alias="monthly_customer_summary",
        materialized="incremental",
        unique_key=["dw_customer_id","summarydate"],
        incremental_strategy="merge",
    )
}}
select dw_customer_id, 
DATE_TRUNC('MONTH',summarydate) summarydate,
SUM(order_count) as order_count,
SUM(order_apd) as order_apd,
CASE 
WHEN SUM(order_apd) > 0 THEN 1 ELSE 0 
END as order_apm,
SUM(ordered_amt) as ordered_amt,
SUM(order_cost_amount) as order_cost_amount,
SUM(order_mrp_amount) as order_mrp_amount,
SUM(products_ordered_qty) as products_ordered_qty,
SUM(products_items_qty) as products_items_qty,
SUM(cancelled_order_count) as cancelled_order_count,
SUM(cancelled_order_amount) as cancelled_order_amount,
SUM(cancelled_order_apd) as cancelled_order_apd,
CASE 
WHEN SUM(cancelled_order_apd) > 0 THEN 1 ELSE 0
END as cancelled_order_apm,
SUM(shipped_order_count) as shipped_order_count,
SUM(shipped_order_amount) as shipped_order_amount,
SUM(shipped_order_apd) as shipped_order_apd,
CASE 
WHEN SUM(shipped_order_apd) > 0 THEN 1 ELSE 0
END as shipped_order_apm,
SUM(payment_apd) as payment_apd,
CASE
WHEN SUM(payment_apd) > 0 THEN 1 ELSE 0
END as payment_apm,
SUM(payment_amount) as payment_amount,
SUM(new_customer_apd) as new_customer_apd,
CASE
WHEN SUM(new_customer_apd) > 0 THEN 1 ELSE 0
END as new_customer_apm,
SUM(new_customer_paid_apd) as new_customer_paid_apd,
CASE 
WHEN SUM(new_customer_paid_apd) > 0 THEN 1 ELSE 0
END as new_customer_paid_apm,
current_timestamp as dw_create_timestamp,
MAX(d1.etl_batch_no) as etl_batch_no,
MAX(d1.etl_batch_date) as etl_batch_date
FROM  {{ ref('daily_customer_summary') }} d1
CROSS JOIN etl_metadata.batch_control e1
GROUP BY 1,2 
ORDER BY 1,2