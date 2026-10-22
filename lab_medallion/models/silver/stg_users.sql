with source as (
    select * from {{ source('bronze', 'users') }}
),

typed as (
    select
        cast(uuid as uuid) as user_id,
        trim(username) as username,
        lower(trim(username)) as username_normalized,
        trim(name) as name,
        upper(trim(sex)) as sex,
        lower(trim(mail)) as email,
        cast(birthdate as date) as birthdate,
        split_part(address, chr(10), 1) as street,
        split_part(address, chr(10), 2) as address_line_2
    from source
),

first_orders as (
    select user_id, min(ordered_at) as first_order_at
    from {{ ref('stg_orders') }}
    group by user_id
),

validated as (
    select
        u.*,
        u.birthdate is not null
        and (
            o.first_order_at is null
            or u.birthdate <= o.first_order_at
        ) as birthdate_is_valid
    from typed u
    left join first_orders o on u.user_id = o.user_id
)

select
    user_id,
    username,
    username_normalized,
    name,
    sex,
    email,
    case when birthdate_is_valid then birthdate end as birthdate,
    birthdate_is_valid,
    street,
    nullif(
        regexp_extract(address_line_2, '^(.+), [A-Z]{2} \d{5}$', 1),
        ''
    ) as city,
    regexp_extract(address_line_2, '([A-Z]{2}) (\d{5})$', 1) as state,
    regexp_extract(address_line_2, '([A-Z]{2}) (\d{5})$', 2) as zip_code
from validated
