with fct_pitch as (
    select * from {{ ref('fct_pitch') }}
),
starting_pitchers as (
	select 
		game_date,
		pitcher,
		inning
	from fct_pitch
	group by game_date, pitcher, inning 
	having inning = 1
)
select * from starting_pitchers