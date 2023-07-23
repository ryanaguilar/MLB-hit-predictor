with stg_pitchers_boxes as (
    select * from {{ ref('stg_box_scores_pitchers') }}
),
stg_pitchers_game_dates as (
    select * from {{ ref('stg_pitcher_game_date_wide') }}
)
select
    spb.*,
    wide.*
from stg_pitchers_boxes as spb
join stg_pitchers_game_dates as wide
on spb.k_pitcher_date = wide.k_pitcher_date