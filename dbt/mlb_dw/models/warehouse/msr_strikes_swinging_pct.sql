with game_data as (
    select * from {{ ref('fct_pitch') }}
),
swstr_cte as (
select 
	game_date,
	pitcher, 
	count(pitcher),
	sum(
	case description
		when 'swinging_strike' then 1
		else 0
	end
	) as swstr
from game_data 
group by pitcher, game_date, game_pk
),
date_pitcher as (
select
 game_date,
 pitcher::int,
 swstr,
 count as pitch_count,
 swstr::float / count::float as swstr_percent
from swstr_cte
),
running_counts as (
select
	game_date,
	pitcher,
	sum(swstr) over (partition by pitcher order by game_date asc rows between unbounded preceding and current row) as running_swstr,
	sum(pitch_count) over (partition by pitcher order by game_date asc rows between unbounded preceding and current row) as running_pitch_count
from date_pitcher
),
percentage as (
select
	pitcher,
	game_date,
	running_swstr,
	running_pitch_count,
	running_swstr / running_pitch_count as swstr_pct
from running_counts
)
select * from percentage