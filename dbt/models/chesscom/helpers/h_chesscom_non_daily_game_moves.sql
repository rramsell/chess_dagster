{% set column_mapping = model.config.get("meta") %}
{% set cols = meta_columns(column_mapping) %}

{% set time_control_increment_seconds_sum %}
    sum(time_control_increment_seconds) over (partition by game_id order by round)
{% endset %}

{% set white_remaining_seconds %}
    extract(epoch from white_clock::interval)
{% endset %}

{% set black_remaining_seconds %}
    extract(epoch from black_clock::interval)
{% endset %}

{% set black_final_turn_timeout %}
    extract(
        epoch from (
            game_end_dt 
                - (
                    round_start_dt 
                        + white_move_duration_seconds * interval '1 second'
            )
        )
    ) = 0
{% endset %}

with mapped as (
        select {{ type_mapper(column_mapping) }}
        from {{ ref('chesscom_games') }}
        cross join lateral jsonb_each(moves) as e(k, v)
        where time_class != 'daily'
    ),
    seconds_parsing as (
        select
            {{ cols | join(", ")}},
            round = 1 as flag_first_round,
            round = max(round) over (partition by game_id) as flag_last_round,
            {{ white_remaining_seconds }} as white_remaining_seconds,
            {{ black_remaining_seconds }} as black_remaining_seconds,
            coalesce(
                -- first round formula:
                -- game timer - time increment and black remaining seconds
                -- other round formula(s):
                -- previous remaining seconds - current remaining seconds
                case
                    when round = 1
                    then time_control_seconds - (
                        {{ white_remaining_seconds }}
                            - time_control_increment_seconds
                    )
                    else
                        lag({{ white_remaining_seconds }}) over (
                            partition by game_id
                            order by round
                        ) - ({{ white_remaining_seconds }} - time_control_increment_seconds)
                    end,
                0
            ) as white_move_duration_seconds,
            coalesce(
                -- first round formula:
                -- game timer - time increment and black remaining seconds
                -- other round formula(s):
                -- previous remaining seconds - current remaining seconds
                case
                    when round = 1 
                    then time_control_seconds - (
                        {{ black_remaining_seconds }}
                            - time_control_increment_seconds
                    )
                    else
                        lag({{ black_remaining_seconds }}) over (
                            partition by game_id
                            order by round
                        ) - ({{ black_remaining_seconds }} - time_control_increment_seconds)
                    end,
                0
            ) as black_move_duration_seconds
        from mapped
    ),
    round_start_and_end_times as (
        select *,
            {{ if(
                "flag_last_round",
                "game_end_dt",
                "round_start_dt 
                    + (
                        white_move_duration_seconds 
                            + black_move_duration_seconds
                    ) * interval '1 second'"
            )}} as round_end_dt
        from (
            select *,
                {{ if(
                    "flag_first_round",
                    "game_start_dt",
                    "game_start_dt + (
                        sum(
                            white_move_duration_seconds 
                                + black_move_duration_seconds
                        ) over (
                            partition by game_id
                            order by round
                            rows between unbounded preceding and 1 preceding
                        ) * interval '1 second'
                    )"
                )}} as round_start_dt
            from seconds_parsing
        ) as round_start
    )
select
    {{ dbt_utils.generate_surrogate_key([
		'game_id',
		'round'
	]) }} as id,
	game_id,
    round,
    flag_first_round,
    flag_last_round,
	round_start_dt,
    round_end_dt,
    extract(epoch from (round_end_dt - round_start_dt)::interval) as round_duration_seconds,
	white_move,
    -- purposefully and only imputes
    -- the last round for black
	{{ if(
        "flag_last_round",
        "coalesce(black_move, 'TERMINAL')",
        "black_move"
    ) }} as black_move,
    white_move_duration_seconds,
    {{ if(
        "flag_last_round",
        "extract(epoch from (round_end_dt - round_start_dt)::interval) - white_move_duration_seconds",
        "black_move_duration_seconds"
    )}} as black_move_duration_seconds,
	white_clock,
	case 
        when flag_last_round
        then coalesce(
            -- prefers black clock if present
            -- essentially means black moved  
            -- before a terminal game state
            black_clock,
            case
                -- cases where black times
                -- out we impute with 0
                when {{ black_final_turn_timeout }}                  
                then '0:00:00.0'
                -- cases where black is mated or abandons
                -- their time left on the clock does not
                -- change from the previous round
                else substring((
                -- last black clock - final round duration - white move duration
                    (
                        lag(black_clock) over (
                            partition by game_id 
                            order by round
                        )
                    )::interval - (
                        (extract(epoch from (round_end_dt - round_start_dt)::interval) 
                            - white_move_duration_seconds) * interval '1 second'
                    )
                )::varchar from 2)
                end
            )
        else black_clock
    end as black_clock,
    white_remaining_seconds,
    case
        when flag_last_round
        then coalesce(
            black_remaining_seconds,
            case
                -- cases where black times
                -- out we impute with 0
                when {{ black_final_turn_timeout }}
                then 0
                -- cases where black is mated or abandons
                -- their time left on the clock does not
                -- change from the previous round
                else extract(epoch from (
                -- last black clock - final round duration - white move duration
                    (
                        lag(black_clock) over (
                            partition by game_id 
                            order by round
                        )
                    )::interval - (
                        (extract(epoch from (round_end_dt - round_start_dt)::interval) 
                            - white_move_duration_seconds) * interval '1 second'
                    )
                ))
            end
        )
        else black_remaining_seconds
    end as black_remaining_seconds
from round_start_and_end_times
where white_clock is not null
