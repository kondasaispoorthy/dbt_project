{{ 
    config(
        alias = "orderdetails",
        materialized = "incremental",
        unique_key = ["src_orderNumber","src_productCode"],
        incremental_strategy = 'merge'
    ) 
}}

WITH t1 AS (
    SELECT COALESCE(MAX(dw_orderdetail_id), 0) AS max_id FROM dev_dw.orderdetails
),
t2 AS (
    SELECT etl_batch_no, etl_batch_date FROM etl_metadata.batch_control
)

SELECT  
    COALESCE(t.dw_orderdetail_id, t1.max_id + ROW_NUMBER() OVER (ORDER BY s.orderNumber, s.productCode)) AS dw_orderdetail_id,
    o.dw_order_id, 
    p.dw_product_id, 
    s.orderNumber AS src_orderNumber,
    s.productCode AS src_productCode,
    s.quantityOrdered,
    s.priceEach,
    s.orderLineNumber,
    s.create_timestamp AS src_create_timestamp,
    s.update_timestamp AS src_update_timestamp,
    COALESCE(t.dw_create_timestamp, CURRENT_TIMESTAMP) AS dw_create_timestamp,
    CURRENT_TIMESTAMP AS dw_update_timestamp,
    COALESCE(t.etl_batch_no, t2.etl_batch_no) AS etl_batch_no,
    COALESCE(t.etl_batch_date, t2.etl_batch_date) AS etl_batch_date
FROM 
    dev_stage.orderdetails s
JOIN {{ ref('products') }} p ON s.productCode = p.src_productCode
JOIN {{ ref('orders') }} o ON s.orderNumber = o.src_orderNumber 
LEFT JOIN dev_dw.orderdetails t ON s.orderNumber = t.src_orderNumber AND s.productCode = t.src_productCode
CROSS JOIN t1
CROSS JOIN t2
