with batter_box as (
    select * from {{ ref('stg_box_scores') }}
)
select
     bb.k_batter,
     bb.player,
     bb.game_date,
     lead(bb.current_game_hits) over (partition by k_batter order by k_batter, game_date) as next_game_hits,
     case
        when lead(bb.current_game_hits) over (partition by k_batter order by k_batter, game_date) > 0 then 1
        else 0
     end as next_game_hit_indicator,
     lead(game_date) over (partition by k_batter order by k_batter, game_date) as next_game_date
from batter_box as bb