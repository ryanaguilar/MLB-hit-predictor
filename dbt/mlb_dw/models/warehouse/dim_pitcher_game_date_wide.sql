with stg_pitchers_boxes as (
    select * from {{ ref('stg_box_scores_pitchers') }}
),
stg_pitchers_game_dates as (
    select * from {{ ref('stg_pitcher_game_date_wide') }}
)
select
    spb.k_pitcher,
    spb.k_pitcher_date,
    spb.strike_outs,
    spb.batters_faced,
    spb.number_of_pitches,
    case
        when spb.batters_faced = 0 then 0
        else (spb.strike_outs::float / spb.batters_faced::float)
    end as strike_out_pct,
    wide.ground_ball_pct,
    wide.swinging_strikes,
    spb.hits,
    case
        when mod(cast(spb.innings_pitched as numeric),0.2) = 0 then floor(spb.innings_pitched) + (2.0/3.0)
        when mod(cast(spb.innings_pitched as numeric),0.1) = 0 then floor(spb.innings_pitched) + (1.0/3.0)
        else spb.innings_pitched
    end as innings_pitched,
    case
        when spb.innings_pitched = 0 then 0
        else (spb.hits::float / spb.innings_pitched::float)
    end as hits_per_inning_pitched,
    case
        when spb.batters_faced = 0 then 0
        else (spb.number_of_pitches::float / spb.batters_faced::float)
    end as pitchers_per_appearance
from stg_pitchers_boxes as spb
join stg_pitchers_game_dates as wide
on spb.k_pitcher_date = wide.k_pitcher_date