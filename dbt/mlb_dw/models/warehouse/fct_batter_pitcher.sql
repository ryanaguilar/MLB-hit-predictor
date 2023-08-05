with batter_pitcher as (
    select * from {{ ref('int_batter_pitcher_association') }}
),
dim_pitcher as (
    select * from {{ ref('dim_pitcher_game_date_wide') }}
),
next_game as (
select
    bp.k_batter,
    bp.k_date,
    bp.game_date,
    lead(k_pitcher) over (partition by k_batter order by k_batter, game_date) as next_game_starting_pitcher,
    lead(game_date) over (partition by k_batter order by k_batter, game_date) as next_game_date
from batter_pitcher as bp
),
box_scores as (
    select * from {{ ref('stg_box_scores') }}
)
select
    ng.k_batter,
    ng.k_date,
    ng.game_date,
    ng.next_game_starting_pitcher,
    ng.next_game_date,
    p.innings_pitched,
    p.strike_out_pct,
    p.ground_ball_pct,
    p.swinging_strikes,
    p.hits_per_inning_pitched,
    p.pitchers_per_appearance,
    bs.sz_babip as babip,
    bs.sz_avg as ba,
    (bs.sz_at_bats - bs.sz_strike_outs - bs.sz_home_runs + bs.sz_sac_flies) as bip
from next_game as ng
left join dim_pitcher as p
on ng.next_game_date = p.game_date
and ng.next_game_starting_pitcher = p.k_pitcher
left join box_scores as bs
on ng.next_game_date = bs.game_date
and ng.k_batter = bs.k_batter