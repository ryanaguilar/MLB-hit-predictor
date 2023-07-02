with bb_total as (
    select * from {{ ref('msr_balls_batted') }}
),
bb_ground_balls as (
    select * from {{ ref('msr_ground_balls_batted') }}
),
aggregated as (
	select 
		bb_ground_balls.game_date,
		bb_ground_balls.pitcher,
		bb_ground_balls.ground_balls_total,
		bb_total.batted_balls_total,
		case
			when bb_total.batted_balls_total = 0 then 0
			else bb_ground_balls.ground_balls_total::float / bb_total.batted_balls_total::float
		end as ground_ball_pct
	from bb_ground_balls
	left join bb_total 
	on bb_ground_balls.game_date = bb_total.game_date
	and bb_ground_balls.pitcher = bb_total.pitcher
)
select * from aggregated
