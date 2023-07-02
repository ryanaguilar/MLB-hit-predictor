with game_data as (
    select * from {{ ref('fct_pitch') }}
),
msr_batters_faced as (
select 
 game_date,
 pitcher,
 batter
from game_data 
group by game_date, pitcher, batter
),
msr_batters_faced_count as (
	select 
		game_date,
		pitcher,
		count(batter) as total_batters_faced
	from msr_batters_faced
	group by game_date, pitcher
),
msr_strikeouts as (
	select 
		game_date,
		pitcher,
		case 
			when events in ('strikeout', 'strikeout_double_play') then 1
			else 0
		end as strikeouts
	from game_data
),
msr_strikeouts_pitcher_game as (
	select
		game_date,
		pitcher,
		sum(strikeouts) as strikeouts_total
	from msr_strikeouts
	group by game_date, pitcher
),
msr_running_totals as (
	select 
		mspg.game_date,
		mspg.pitcher,
		sum(mbf.total_batters_faced) over (partition by mspg.pitcher order by mspg.game_date asc rows between unbounded preceding and current row) as batters_faced_running_total,
		sum(mspg.strikeouts_total) over (partition by mspg.pitcher order by mspg.game_date asc rows between unbounded preceding and current row) as strikeouts_running_total
	from msr_strikeouts_pitcher_game mspg
	left join msr_batters_faced_count mbf 
	on mspg.game_date = mbf.game_date
	and mspg.pitcher = mbf.pitcher
),
aggregated as (
	select
		game_date,
		pitcher,
		case 
			when strikeouts_running_total = 0 then 0
			else strikeouts_running_total::float / batters_faced_running_total::float 
		end  as strikeout_pct
	from msr_running_totals
)
--select count(*) from msr_strikeouts_pitcher_game; --6201
--select count(*) from msr_batters_faced_count; --6201
--select count(*) from msr_running_totals; --6201
select * from aggregated