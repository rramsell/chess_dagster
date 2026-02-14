-- 
select id, username, start_dt, end_dt
from {{ ref("chesscom_games") }}
where 
    time_class != 'daily'
    and start_dt is not null
    and end_dt is not null

union
-- treating daily game moves as a 'login'
-- white sessions
select gm.id, gm.username, gm.round_start_dt as start_dt, gm.round_start_dt as end_dt
from {{ ref("h_chesscom_daily_game_moves") }} as gm
inner join 
    {{ ref("chesscom_games") }} as g on 
    g.id = gm.game_id 
    and g.white_username = gm.username
    and gm.round_start_dt is not null

union
-- black sessions
select gm.id, gm.username, gm.round_end_dt as start_dt, gm.round_end_dt as end_dt
from {{ ref("h_chesscom_daily_game_moves") }} as gm
inner join 
    {{ ref("chesscom_games") }} as g on 
    g.id = gm.game_id 
    and g.black_username = gm.username
    and gm.black_move != 'TERMINATION'
    and gm.round_end_dt is not null
