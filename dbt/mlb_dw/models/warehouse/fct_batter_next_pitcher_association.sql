with batter_pitcher as (
    select * from {{ ref('int_batter_pitcher_association') }}
)
select
     bp.k_batter,
     bp.k_date,
     bp.game_date,
     lead(k_pitcher) over (partition by k_batter order by k_batter, game_date) as next_game_starting_pitcher,
     lead(game_date) over (partition by k_batter order by k_batter, game_date) as next_game_date
from batter_pitcher as bp