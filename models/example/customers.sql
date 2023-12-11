{{
    config(
        alias="customers",
        materialized="incremental",
        unique_key=["dw_customer_id"],
        incremental_strategy="merge",
    )
}}
with t1 as (select coalesce(max(dw_customer_id), 0) as max_id from dev_dw.customers)

select
    coalesce(
        dw_customer_id, t1.max_id + row_number() over (order by s.customernumber)
    ) as dw_customer_id,
    s.customernumber as customernumber,
    s.customername,
    s.contactlastname,
    s.contactfirstname,
    s.phone,
    s.addressline1,
    s.addressline2,
    s.city,
    s.state,
    s.postalcode,
    s.country,
    c.dw_employee_id,
    s.salesrepemployeenumber,
    s.creditlimit,
    s.create_timestamp as src_create_timestamp,
    s.update_timestamp as src_update_timestamp,
    current_timestamp as dw_create_timestamp,
    current_timestamp as dw_update_timestamp,
    coalesce(d.etl_batch_no,e1.etl_batch_no) as etl_batch_no,
    coalesce(d.etl_batch_date,e1.etl_batch_date) as etl_batch_date
from dev_stage.customers s
left join dev_dw.customers d on s.customernumber = d.customernumber
LEFT JOIN {{ ref('employees') }} c ON
s.salesRepEmployeeNumber = c.employeeNumber
cross join etl_metadata.batch_control e1
cross join t1
