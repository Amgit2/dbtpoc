with base as (
  select
    utdetailsid::number                         as utdid,
    transactionid::number                       as transactionid,
    nullif(trim(employername), '')              as employername,
    nullif(trim(employeename), '')              as employeename,
    try_to_date(transactiondate)                as transaction_date,
    try_to_time(transactiontimepst)             as transaction_time_pst,
    upper(nullif(trim(transactionauthday), '')) as transaction_auth_day,
    upper(nullif(trim(transactionstatus), ''))  as txn_status,
    accessibleamount::number             as accessible_amount,
    transactionamount::number            as transaction_amount,
    serviceid::number                           as service_id,
    deductions::number                   as deductions,
    emoloyeefee::number                  as employee_fee,
    employerfeeamount::number            as employer_fee_amount,
    totalfeelamount::number              as total_fee_amount,
    bid::number                                 as bid,
    buid::number                                as buid,
    lower(nullif(trim(env), ''))                as env
  from {{source("raw", "transactionswdetails")}}
),
filtered as (
  select *
  from base
  where employername is not null
    and transaction_date is not null and transaction_date <= current_date()
    and transaction_time_pst is not null
    and transaction_auth_day in ('MON','TUE','WED','THU','FRI','SAT','SUN')
    and txn_status in ('AUTHORIZED','DECLINED','SETTLED','REVERSED','PENDING')
    and transaction_amount > 0
    and accessible_amount >= 0
    and buid > 0
    and env in ('dev','qa','staging','prod')
)
select * from filtered
