{{
    config(
        alias="customer_history",
        materialized="incremental",
        unique_key=["dw_customer_id","effective_from_date"],
        incremental_strategy="merge",
    )
}}
select 
a.dw_customer_id,
a.creditLimit as creditLimit,
a.effective_from_date as effective_from_date,
DATEADD(DAY,-1,e1.etl_batch_date) as effective_to_date, 
0 as dw_active_record_ind,
COALESCE(current_timestamp, b.dw_create_timestamp) as dw_create_timestamp,
current_timestamp as dw_update_timestamp,
COALESCE(a.create_etl_batch_no,e1.etl_batch_no) as create_etl_batch_no,
COALESCE(a.create_etl_batch_date,e1.etl_batch_date)as create_etl_batch_date,
e1.etl_batch_no as update_etl_batch_no,
e1.etl_batch_date as update_etl_batch_date 
FROM  dev_dw.customer_history a JOIN {{ ref('customers') }} b
ON a.dw_customer_id = b.dw_customer_id
CROSS JOIN etl_metadata.batch_control e1
WHERE a.creditLimit <> b.creditLimit
AND a.dw_active_record_ind  = 1
UNION 
select c.dw_customer_id as dw_customer_id,
c.creditLimit as creditLimit,
e1.etl_batch_date as effective_from_date,
NULL as effective_to_date,
1 as  dw_active_record_ind,
current_timestamp as dw_create_timestamp,
current_timestamp as dw_update_timestamp, 
e1.etl_batch_no as create_etl_batch_no,
e1.etl_batch_date as create_etl_batch_date,
NULL as update_etl_batch_no,
NULL as update_etl_batch_date
FROM 
{{ ref('customers') }} c LEFT JOIN dev_dw.customer_history d
ON c.dw_customer_id = d.dw_customer_id 
CROSS JOIN etl_metadata.batch_control e1
where (d.dw_customer_id is null) or (d.creditlimit <> c.creditlimit and d.dw_active_record_ind = 1) 

