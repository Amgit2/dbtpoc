-- Staging/cleaning for RAW.MAXAMOUNTAVAILABLE
-- Assumes you declared the source in src_raw.yml as:
-- sources: { name: raw, tables: [ { name: maxamountavailable, loaded_at_field: launchdate } ] }

with base as (
  select
    UTDETAILSID                      as utdid,
    transactionid                    as tid,
    nullif(trim(employername), '')                  as employername,
    nullif(trim(employeename), '')                  as employeename,
    transactiondate                                 as txn_date,
    --TRANSACTIONTIMEPST                              as txn_datetime,
    TRANSACTIONAUTHDAY                              as txn_authday, 
    TRANSACTIONSTATUS                               as txn_status,
    TRANSACTIONAMOUNT                               as txn_amount,
    accessibleamount                               as accessible_amount,
    serviceid                                       as sid,
    deductions                                      as deductions,
    employerfeeamount,                               
    totalfeelamount                                as totalfeeamount, 
    bid,
    buid
  from {{source("raw", "maxamountavailable")}}
),
filtered as (
  select *
  from base
  where bid is not null and bid > 0
    and employeename is not null
    and employername is not null
    and txn_amount is not null and txn_amount >= 0
    and accessible_amount is not null and accessible_amount >= 0
    --and txn_datetime is not null and txn_datetime <= current_timestamp()

),
normalized as (
  select
        *,
        case when accessible_amount >= txn_amount then true else false end
      as dq_available_le_prev
  from filtered
)
select * from normalized