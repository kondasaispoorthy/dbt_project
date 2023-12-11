{{
    config(
        alias="productlines",
        materialized="incremental",
        unique_key=["dw_product_line_id"],
        incremental_strategy="merge",
    )
}}
with t1 as (select coalesce(max(dw_product_line_id), 0) as max_id from dev_dw.productlines)
SELECT 
COALESCE(dw_product_line_id,t1.max_id + row_number() over (order by a.productLine)) as dw_product_line_id,
a.productLine,
a.create_timestamp as src_create_timestamp,
a.update_timestamp as src_update_timestamp,
coalesce(b.dw_create_timestamp,current_timestamp) as dw_create_timestamp,
current_timestamp as dw_update_timestamp,
e1.etl_batch_no as etl_batch_no,
e1.etl_batch_date as etl_batch_date
FROM
dev_stage.productlines a LEFT JOIN dev_dw.productlines b
ON a.productLine = b.productLine
cross join t1
cross join etl_metadata.batch_control e1
