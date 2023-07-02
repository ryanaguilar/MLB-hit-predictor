with fct_pitch as (
    select * from {{ ref('fct_pitch') }}
),
ground_balls_batted_indicator as (
	select 
		game_date,
		pitcher,
		case
			when bb_type = 'ground_ball' then 1
			else 0
		end as ground_balls
	from game_data
),
aggregated as ( 
	select 
		game_date,
		pitcher,
		sum(ground_balls) as ground_balls_total
	from ground_balls_batted_indicator
	group by game_date, pitcher
)
select * from aggregated