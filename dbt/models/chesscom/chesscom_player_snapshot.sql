{% set column_mapping = model.config.get("meta") %}
{% set cols = meta_columns(column_mapping, exclude=["last_online_dt"]) %}

with mapped as (
        select {{ type_mapper(column_mapping) }}
        from {{ source("src_chesscom", "player") }}
    ),
    last_game_impute as (
        select src.username, src.ingested_dt, max(games.end_dt) as last_game_dt
        from mapped as src
        inner join 
            {{ ref("chesscom_games") }} as games on
            games.username = src.username
            and games.end_dt <= src.ingested_dt
        {{ dbt_utils.group_by(n=2) }}
    )
select
    {{ dbt_utils.generate_surrogate_key([
        "src.username",
        "src.ingested_dt"
    ]) }} as id,
    {% for col in cols %}
        src.{{ col }},
    {% endfor %}
    row_number() over (
        partition by src.username 
        order by src.ingested_dt desc
    ) = 1 as flag_latest_snapshot,
    {{ greatest_or_least(
        "greatest", 
        fields=["game.last_game_dt", "src.last_online_dt"]
    ) }} as last_online_dt
from mapped as src
left join 
    last_game_impute as game on 
    game.username = src.username
    and game.ingested_dt = src.ingested_dt

