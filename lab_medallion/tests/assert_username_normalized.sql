select user_id, username_normalized
from {{ ref('stg_users') }}
where username_normalized <> lower(trim(username))
