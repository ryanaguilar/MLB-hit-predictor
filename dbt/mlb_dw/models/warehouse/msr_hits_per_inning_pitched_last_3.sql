with msr_innings_pitched as (
    select * from {{ ref('msr_innings_pitched') }}
),
msr_hit_indicator as (
	select 
		game_date,
		pitcher,
		--player_name,
		case 
			when events in ('single', 'double', 'triple', 'home_run') then 1
			else 0
		end as hit_counter
	from game_data
	--,player_name 
),
msr_hits as (
	select 
		game_date,
		pitcher,
		--player_name,
		sum(hit_counter) as hits
	from msr_hit_indicator
	group by game_date, pitcher 
	--,player_name 
),
msr_hits_per_innings_pitched as (
select 
	mip.game_date,
	mip.pitcher,
	h.hits,
	case 
		when mip.innings_pitched = 0 then (h.hits + lag(h.hits) over (partition by mip.pitcher order by mip.game_date asc rows between unbounded preceding and current row)) / lag(mip.innings_pitched) over (partition by mip.pitcher order by mip.game_date asc rows between unbounded preceding and current row)
		else h.hits / mip.innings_pitched
	end as hip
from msr_innings_pitched mip
join msr_hits as h
on mip.game_date = h.game_date
and mip.pitcher = h.pitcher
),
aggregated as (
	select 
	mhip.*,
	avg(hip) over (partition by pitcher order by game_date asc rows between unbounded preceding and current row) as hip_last3
	--,sum(hip) over (order by game_date) as hip_sum
	from msr_hits_per_innings_pitched mhip
)
select * from aggregated