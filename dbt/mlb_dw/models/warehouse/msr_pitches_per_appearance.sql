with game_data as (
    select * from {{ ref('fct_pitch') }}
),
pitches_per_game as (
select 
	pitcher,
	game_date,
	count(pitcher)
from fct_pitch
group by pitcher, game_date
),
averaged as (
    select
        game_date,
        pitcher,
        avg(count) as pitches_per_appearance
    from pitches_per_game
    group by game_date, pitcher
)
select * from averaged