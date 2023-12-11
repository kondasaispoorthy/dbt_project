{{
    config(
        alias="product_history",
        materialized="incremental",
        unique_key=["dw_product_id","effective_from_date"],
        incremental_strategy="merge",
    )
}}
select 
a.dw_product_id,
a.msrp,
a.effective_from_date,
DATEADD(DAY,-1,e1.etl_batch_date) as effective_to_date,
0 as dw_active_record_ind,
a.dw_create_timestamp,
current_timestamp as dw_update_timestamp,
a.create_etl_batch_no,
a.create_etl_batch_date,
e1.etl_batch_no as update_etl_batch_no,
e1.etl_batch_date as update_etl_batch_date
FROM dev_dw.product_history a JOIN {{ ref('products') }} b 
ON a.dw_product_id = b.dw_product_id
CROSS JOIN etl_metadata.batch_control e1
WHERE a.MSRP <> b.MSRP
AND a.dw_active_record_ind  = 1
UNION 
SELECT
c.dw_product_id,
c.msrp,
e1.etl_batch_date as effective_from_date,
NULL AS effective_to_date,
1 as dw_active_record_ind,
current_timestamp as dw_create_timestamp,
current_timestamp as dw_update_timestamp,
e1.etl_batch_no as create_etl_batch_no,
e1.etl_batch_date as create_etl_batch_date,
NULL AS update_etl_batch_no,
NULL AS update_etl_batch_date
FROM {{ ref('products') }} c LEFT JOIN dev_dw.product_history d
ON  c.dw_product_id = d.dw_product_id
CROSS JOIN etl_metadata.batch_control e1
where (d.dw_product_id is null) or (d.msrp <> c.msrp and d.dw_active_record_ind = 1) 

 



