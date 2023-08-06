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
),
next_game_hit as (
    select * from {{ ref('fct_next_game_hit')}}
)
select
    ng.k_batter,
    bs.player,
    ng.k_date,
    ng.game_date,
    ng.next_game_starting_pitcher,
    ng.next_game_date,
    p.innings_pitched,
    p.strike_out_pct as strikeout_percentage,
    p.ground_ball_pct as ground_ball_percentage,
    p.swinging_strikes as Strikes_Swinging,
    p.hits_per_inning_pitched,
    p.pitchers_per_appearance as pitches_per_appearance_avg,
    bs.sz_babip as BABIP,
    bs.sz_avg as BA,
    (bs.sz_at_bats - bs.sz_strike_outs - bs.sz_home_runs + bs.sz_sac_flies) as BIP,
    p.last_three_innings_pitched as hip_last_three,
    ngh.next_game_hits,
    ngh.next_game_hit_indicator as next_game_HIT
from next_game as ng
left join dim_pitcher as p
on ng.next_game_date = p.game_date
and ng.next_game_starting_pitcher = p.k_pitcher
left join box_scores as bs
on ng.next_game_date = bs.game_date
and ng.k_batter = bs.k_batter
left join next_game_hit ngh
on ng.k_batter = ngh.k_batter
and ng.game_date = ngh.game_date