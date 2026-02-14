{{ dbt_utils.union_relations(
    relations=[
    ref('h_chesscom_daily_game_moves'),
    ref('h_chesscom_non_daily_game_moves')
    ],
    source_column_name=None
) }}