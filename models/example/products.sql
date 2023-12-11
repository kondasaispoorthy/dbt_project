{{
    config(
        alias="products",
        materialized="incremental",
        unique_key=["dw_product_id"],
        incremental_strategy="merge",
    )
}}
with t1 as (select coalesce(max(dw_product_id), 0) as max_id from dev_dw.products)
SELECT
COALESCE(dw_product_id,t1.max_id + ROW_NUMBER() OVER(ORDER BY a.productCode)) as dw_product_id,
a.productCode as src_productcode,
a.productName,
a.productLine,
a.productScale,
a.productvendor,
a.quantityInStock,
a.buyPrice,
a.MSRP,
c.dw_product_line_id,
a.create_timestamp as src_create_timestamp,
a.update_timestamp as src_update_timestamp,
COALESCE(b.dw_create_timestamp,current_timestamp) as dw_create_timestamp,
current_timestamp as dw_update_timestamp,
COALESCE(b.etl_batch_no,e1.etl_batch_no) as etl_batch_no,
COALESCE(b.etl_batch_date,e1.etl_batch_date) as etl_batch_date
FROM
dev_stage.products a LEFT JOIN dev_dw.products b 
ON a.productCode = b.src_productCode
JOIN {{ ref('productlines') }} c ON
a.productLine = c.productLine
cross join t1
cross join etl_metadata.batch_control e1