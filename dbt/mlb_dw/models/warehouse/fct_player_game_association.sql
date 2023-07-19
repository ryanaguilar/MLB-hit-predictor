with stg_player_game as (
    select * from {{ ref('stg_player_game_associations') }}
),
formatted as (
    select 
        stg_player_game.k_player,
        stg_player_game.k_player_game,
        stg_player_game.game_date,
        stg_player_game.sz_avg as batting_average,
        stg_player_game.sz_hits as hits,
        stg_player_game.sz_plate_appearances as plate_appearances,
        stg_player_game.sz_runs as runs,
        COALESCE(stg_player_game.current_game_hits, 0) as current_game_hits,
        (stg_player_game.sz_at_bats - stg_player_game.sz_strike_outs - stg_player_game.sz_home_runs + stg_player_game.sz_sac_flies) as bip,
        ((stg_player_game.sz_hits - stg_player_game.sz_home_runs)::float / (stg_player_game.sz_at_bats - stg_player_game.sz_strike_outs - stg_player_game.sz_home_runs + stg_player_game.sz_sac_flies)::float) as babip,
        case
            when lead(current_game_hits) over (partition by k_player order by game_date) > 0 then 1
            else 0
        end as next_game_hit
    from stg_player_game
)
select * from formatted