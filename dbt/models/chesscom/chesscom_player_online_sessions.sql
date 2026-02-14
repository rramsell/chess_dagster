{% set connected_session_threshold = 600 %}
{% set login_padding %}
    60 * interval '1 second'
{% endset %}

with player_and_game_sessions as (
        {{ dbt_utils.union_relations(
            relations=[
            ref('h_chesscom_player_snapshot_logins'),
            ref('h_chesscom_games_sessions')
            ],
            include=['id', 'username', 'start_dt', 'end_dt']
        ) }}
	),
	session_windows as (
		select
			id,
			username,
			start_dt,
			end_dt,
			lead(start_dt) over (partition by username order by start_dt) as next_session_start_dt,
			lag(end_dt) over (partition by username order by start_dt) as previous_session_end_dt
		from player_and_game_sessions
	),
	connected_sessions as (
		-- assumes online/games connected if there's <= 10 min between games
		select *, (flag_connected_next or flag_connected_previous) as flag_connected
		from (
			select
				id,
				username,
				start_dt,
				end_dt,
				coalesce(
                    {{ connected_session_threshold }} >=
					    extract(epoch from (start_dt - previous_session_end_dt)), 
					false
				) as flag_connected_previous,
				coalesce(
					{{ connected_session_threshold }} >=
                        extract(epoch from (next_session_start_dt - end_dt)),
					false
				) as flag_connected_next
			from session_windows
		) as conn
	),
	session_buckets as (
		select *,
			-- padding the front and back of a session w/ 30 seconds
			-- to account for login and game review time spent
			min(start_dt) over (
                partition by username, bucket_number
            ) - {{ login_padding }} as session_start_dt,
			max(end_dt) over (
                partition by username, bucket_number
            ) + {{ login_padding }} as session_end_dt
		from (
			select
				id,
				username,
				start_dt,
				end_dt,
				sum(
					case 
						when flag_connected_previous is distinct from true
						then 1
						else 0
					end
				) over (
					partition by username
					order by start_dt
				) as bucket_number
			from connected_sessions
		) as buckets
	)
select
    {{ dbt_utils.generate_surrogate_key([
        "username",
        "bucket_number::varchar"
    ]) }} as id,
	username,
	bucket_number::varchar as session_id,
	session_start_dt,
	session_end_dt,
	min(
        extract(
            epoch from (
                session_end_dt - session_start_dt
            )
        )
    ) as session_duration_seconds
from session_buckets
{{ dbt_utils.group_by(n=5) }}
