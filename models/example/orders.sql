{{
    config(
        alias="orders",
        materialized="incremental",
        unique_key=["dw_order_id"],
        incremental_strategy="merge",
    )
}}
WITH t1 AS (
    SELECT COALESCE(MAX(dw_order_id), 0) AS max_id FROM dev_dw.orders
)
SELECT 
    COALESCE(b.dw_order_id, t1.max_id + ROW_NUMBER() OVER (ORDER BY a.orderNumber)) AS dw_order_id,
    c.dw_customer_id,
    a.orderNumber AS src_orderNumber,
    a.orderDate,
    a.requiredDate,
    a.shippedDate,
    a.status,
    a.comments,
    a.customerNumber AS src_customernumber,
    a.create_timestamp AS src_create_timestamp,
    a.update_timestamp AS src_update_timestamp,
    COALESCE(b.dw_create_timestamp, CURRENT_TIMESTAMP) AS dw_create_timestamp,
    CURRENT_TIMESTAMP AS dw_update_timestamp, 
    a.cancelledDate,
    COALESCE(b.etl_batch_no, e1.etl_batch_no) AS etl_batch_no,
    COALESCE(b.etl_batch_date, e1.etl_batch_date) AS etl_batch_date
FROM 
    dev_stage.orders a 
LEFT JOIN dev_dw.orders b ON a.orderNumber = b.src_orderNumber
JOIN {{ ref('customers') }} c ON a.customerNumber = c.customerNumber
CROSS JOIN t1
CROSS JOIN etl_metadata.batch_control e1
