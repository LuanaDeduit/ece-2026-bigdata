{{ config(severity='warn') }}

select
    u.user_id,
    u.state,
    u.zip_code
from {{ ref('stg_users') }} u
where exists (
    select 1
    from {{ ref('zip_state_reference') }} r
    where r.zip_code = u.zip_code
)
and not exists (
    select 1
    from {{ ref('zip_state_reference') }} r
    where r.zip_code = u.zip_code
      and r.state_code = u.state
)
