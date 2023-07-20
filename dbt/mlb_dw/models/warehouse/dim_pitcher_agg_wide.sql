with fct_pitch as (
    select * from {{ ref('fct_pitch') }}
),
counts as (
	select 
		game_date,
		pitcher,
		count(bb_type) as batted_balls_total,
        count(pitcher) filter (where bb_type = 'ground_ball') as ground_balls_total,
        count(pitcher) filter (where events in ('single', 'double', 'triple', 'home_run')) as hits
	from fct_pitch
	group by game_date, pitcher
),
aggregated as (
    select
        max(game_date),
        pitcher,
        sum(batted_balls_total) as balls_batted,
        sum(ground_balls_total) as ground_balls,
        case
			when sum(batted_balls_total) = 0 then 0
			else sum(ground_balls_total)::float / sum(batted_balls_total)::float
		end as ground_ball_pct,
        sum(hits)
        
    from counts
    group by pitcher
) 
select * from aggregated