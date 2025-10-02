with
    base as (
        select
            -- id::number                      as id,
            bid::number as bid,
            -- buid::number                    as buid,
            nullif(trim(empname), '') as employername,
            transactionruleid as trid,
            rulename as rulename,
            launchdate::datetime as record_ts,
            upper(nullif(trim(phase), '')) as phase,
            -- nullif(trim(model), '')         as model,
            lower(nullif(trim(env), '')) as env,
        -- nullif(trim(ubid), '')          as ubid
        from {{ source("raw", "employerpricing") }}

    ),
    filtered as (
        select *
        from base
        where
            employername is not null
            and record_ts is not null
            and record_ts <= current_timestamp()
            and phase in ('ALPHA', 'BETA', 'GA', 'SUNSET')
            and env in ('dev', 'qa', 'staging', 'prod')
            and bid > 0
    )
select *
from filtered
