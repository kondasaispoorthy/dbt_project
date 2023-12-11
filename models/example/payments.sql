{{
    config(
        alias="payments",
        materialized="incremental",
        unique_key=["dw_payment_id"],
        incremental_strategy="merge",
    )
}}
WITH t1 AS (
    SELECT COALESCE(MAX(dw_payment_id), 0) AS max_id FROM dev_dw.payments
)
SELECT 
    COALESCE(b.dw_payment_id, t1.max_id + ROW_NUMBER() OVER (ORDER BY a.checkNumber)) AS dw_payment_id,
    c.dw_customer_id,
    a.customerNumber,
    a.checkNumber,
    a.paymentDate,
    a.amount,
    a.create_timestamp AS src_create_timestamp,
    a.update_timestamp AS src_update_timestamp,
    COALESCE(b.dw_create_timestamp, CURRENT_TIMESTAMP) AS dw_create_timestamp,
    CURRENT_TIMESTAMP AS dw_update_timestamp,
    COALESCE(b.etl_batch_no, e1.etl_batch_no) AS etl_batch_no,
    COALESCE(b.etl_batch_date, e1.etl_batch_date) AS etl_batch_date
FROM
    dev_stage.payments a
JOIN {{ ref('customers') }} c ON a.customerNumber = c.customerNumber
LEFT JOIN dev_dw.payments b ON a.checkNumber = b.checkNumber AND a.customerNumber = b.customerNumber
CROSS JOIN etl_metadata.batch_control e1
CROSS JOIN t1
