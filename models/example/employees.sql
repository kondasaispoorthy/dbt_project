{{
    config(
    alias = "employees",
    materialized = "incremental",
    unique_key = ["employeenumber"],
    incremental_strategy = 'merge',
    )
}}


with batchcontrol as (SELECT etl_batch_no, etl_batch_date FROM etl_metadata.batch_control),

rownum as (select coalesce(max(dw_employee_id),0) as maxid from dev_dw.employees),

details as (
select coalesce(t.dw_employee_id,r.maxid + ROW_NUMBER() OVER (ORDER BY s.employeenumber))  dw_employee_id,
          s.employeeNumber,
          s.lastName,
          s.firstName,
          s.extension,
          s.email,
          s.officeCode,
          s.reportsTo,
          s.jobTitle,
          w.dw_office_id,
          t.dw_reporting_employee_id,
          s.create_timestamp as src_create_timestamp,
          s.update_timestamp as src_update_timestamp,
          t.dw_create_timestamp as dw_create_timestamp,
          current_timestamp as dw_update_timestamp,
          b.etl_batch_no  as etl_batch_no,
          b.etl_batch_date as etl_batch_date

from dev_stage.employees s
left join {{ ref('offices') }} w
on s.officeCode = w.officeCode
left join dev_dw.employees t
on s.employeeNumber = t.employeeNumber
cross join batchcontrol b
cross join rownum r
)
select 
          s.dw_employee_id,
          s.employeeNumber,
          s.lastName,
          s.firstName,
          s.extension,
          s.email,
          s.officeCode,
          s.reportsTo,
          s.jobTitle,
          s.dw_office_id,
          coalesce(s.dw_reporting_employee_id,e2.dw_employee_id) as  dw_reporting_employee_id ,
          s.src_create_timestamp,
          s.src_update_timestamp,
          coalesce(s.dw_create_timestamp,current_timestamp) as dw_create_timestamp,
          current_timestamp as dw_update_timestamp,
          s.etl_batch_no,
          s.etl_batch_date
from details s
left join details e2
on s.reportsTo = e2.employeeNumber
