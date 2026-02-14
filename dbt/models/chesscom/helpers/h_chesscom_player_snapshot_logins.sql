{% set column_mapping = model.config.get("meta") %}

with ordered as (
        select 
            {{ type_mapper(column_mapping) }},
            row_number() over (
                partition by username, last_online_dt 
                order by last_online_dt desc
            ) as rn
        from {{ ref("chesscom_player_snapshot") }}
        where last_online_dt is not null
    )
select
    id,
    username,
    last_online_dt as start_dt,
    last_online_dt as end_dt
from ordered
where rn = 1
