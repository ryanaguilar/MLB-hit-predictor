with fct_pitch as (
    select * from {{ ref('fct_pitch') }}
),
aggregated as (
	select 
		game_date,
		pitcher,
		count(bb_type) as batted_balls_total
	from fct_pitch
	group by game_date, pitcher
)
select * from aggregated