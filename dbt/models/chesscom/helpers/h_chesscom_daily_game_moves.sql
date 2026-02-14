{% set column_mapping = model.config.get("meta") %}
{% set cols = meta_columns(column_mapping, exclude=["white_clock", "black_clock", "white_move", "black_move", "game_end_dt", "time_control_seconds"]) %}

{% set white_ttm %}
    extract(epoch from white_clock::interval)
{% endset %}

{% set black_ttm %}
    coalesce(extract(epoch from black_clock::interval), 0)
{% endset %}

{% set round_start %}
    game_end_dt - 
		sum({{ white_ttm }} + {{ black_ttm }}) over (partition by game_id order by round desc) * interval '1 second'
{% endset %}

with mapped as (
        select {{ type_mapper(column_mapping) }}
        from {{ ref('chesscom_games') }}
        cross join lateral jsonb_each(moves) as e(k, v)
        where time_class = 'daily'
    )
select
    {{ dbt_utils.generate_surrogate_key(["game_id", "round"])}} as id,
    {{ cols | join(", ")}},
    white_clock,
    {{ if(
        "round = max(round) over (partition by game_id)",
        "coalesce(black_clock, '0:00:00.0')",
        "black_clock"
    )}} as black_clock,
    white_move,
    {{ if(
        "round = max(round) over (partition by game_id)",
        "coalesce(black_move, 'TERMINAL')",
        "black_move"
    )}} as black_move,
    {{ round_start }} as round_start_dt,
	{{ round_start }} + (({{ white_ttm }} + {{ black_ttm }}) * interval '1 second') as round_end_dt,
    round = 1 as flag_first_round,
    round = max(round) over (partition by game_id) as flag_last_round,
    time_control_seconds - {{ white_ttm }} as white_remaining_seconds,
    time_control_seconds - {{ black_ttm }} as black_remaining_seconds,
    {{ white_ttm }} as white_move_duration_seconds,
    {{ black_ttm }} as black_move_duration_seconds,
    {{ white_ttm }} + {{ black_ttm }} as round_duration_seconds
from mapped
where white_clock is not null
