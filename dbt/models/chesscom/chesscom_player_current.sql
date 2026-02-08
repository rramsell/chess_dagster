{% set src = ref('chesscom_player_snapshot') %}

select {{ dbt_utils.star(from=src, except=['id','flag_latest_snapshot']) }}
from {{ src }}
where flag_latest_snapshot
