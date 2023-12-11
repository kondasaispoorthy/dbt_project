{{
    config(
        alias="offices",
        materialized="incremental",
        unique_key=["dw_office_id"],
        incremental_strategy="merge",
    )
}}
with t1 as (select coalesce(max(dw_office_id), 0) as max_id from dev_dw.offices)
select
    coalesce(
        b.dw_office_id, t1.max_id + row_number() over (order by a.officecode)
    ) as dw_office_id,
    a.officecode,
    a.city,
    a.phone,
    a.addressline1,
    a.addressline2,
    a.state,
    a.country,
    a.postalcode,
    a.territory,
    a.create_timestamp as src_create_timestamp,
    a.update_timestamp as src_update_timestamp,
    current_timestamp as dw_create_timestamp,
    current_timestamp as dw_update_timestamp,
    coalesce(b.etl_batch_no,e1.etl_batch_no) as etl_batch_no,
    coalesce(b.etl_batch_date,e1.etl_batch_date) as etl_batch_date
from dev_stage.offices a
left join dev_dw.offices b on a.officecode = b.officecode
cross join etl_metadata.batch_control e1
cross join t1
