{% set column_mapping = model.config.get("meta") %}
{% set excl = ["end_dt", "time_control"] %}
{% set cols = meta_columns(column_mapping, exclude=(excl + ["start_dt", "pgn_start_time", "moves"])) %}

with mapped as (
        select
            {{ type_mapper(column_mapping) }}
        from {{ source('src_chesscom', 'games') }}
    ),
	game_start as (
		select 
			{{ cols | join(", ") }},
			coalesce(
				start_dt,
				{{ if(
					"extract(hour from end_dt)::int < substring(pgn_start_time,1,2)::int",
					"end_dt::date + pgn_start_time::time - interval '1 day'",
					"end_dt::date + pgn_start_time::time"
				)}}
			) as start_dt,
			{{ excl | join(", ")}},
			moves
		from mapped
	)
select
	{{ dbt_utils.generate_surrogate_key([
		'username',
		'game_url',
		'uuid'
	]) }} as id,
	end_dt = min(end_dt) over (partition by username) as flag_first_game,
	end_dt = max(end_dt) over (partition by username) as flag_latest_game,
	{{ (["start_dt"] + excl + cols) | join(", ") }},
	case
		when time_control ~ '^\d+/\d+$'
		then split_part(time_control, '/', 2)::bigint
		when time_control ~ '^\d+\+\d+$'
		then split_part(time_control, '+', 1)::bigint
		when time_control ~ '^\d+$'
		then time_control::bigint
		else null::bigint
	end as time_control_seconds,
	{{ if(
		"time_control ~ '^\d+\+\d+$'",
		"split_part(time_control, '+', 2)::bigint",
		"0::bigint"
	) }} as time_control_increment_seconds,
	moves
from game_start
