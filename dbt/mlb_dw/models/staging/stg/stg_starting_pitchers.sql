with base as (
    select * from {{ ref('base_pitches') }}
),
starting_pitchers as (
	select 
		game_date,
		pitcher,
		batter,
		inning
	from base
	group by game_date, pitcher, batter, inning 
	having inning = 1
)
select * from starting_pitchers