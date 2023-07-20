with fct_pitch as (
    select * from {{ ref('fct_pitch') }}
),
starting_pitchers as (
	select 
		game_date,
		pitcher,
		batter,
		inning
	from fct_pitch
	group by game_date, pitcher, batter, inning 
	having inning = 1
)
select * from starting_pitchers