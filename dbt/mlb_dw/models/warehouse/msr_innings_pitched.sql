with game_data as (
    select * from {{ ref('fct_pitch') }}
),
msr_outs as (
select 
	game_date,
	pitcher,
	inning,
	batter,
	events,
	case
		when events = 'triple_play' then 3
		when events in ('runner_double_play','double_play', 'grounded_into_double_play', 'sac_fly_double_play', 'strikeout_double_play') then 2
		when events in ('other_out', 'sac_bunt', 'sac_fly', 'caught_stealing_2b', 'force_out','strikeout','pickoff_3b', 'pickoff_1b', 'pickoff_caught_stealing_3b','field_out', 'fielders_choice_out', 'pickoff_2b', 'caught_stealing_3b') then 1
		else 0
	end as outs
from game_data
),
msr_total_outs as (
select
	game_date,
	pitcher,
	inning,
	sum(outs) as outs
from msr_outs
group by game_date, pitcher, inning
order by game_date, pitcher, inning
),
msr_last_inning as (
select 
 game_date,
 pitcher,
 max(inning) as last_inning
from msr_total_outs mto
group by game_date, pitcher
),msr_last_inning_and_outs as (
select 
	mli.game_date,
	mli.pitcher,
	mli.last_inning,
	mto.outs as last_inning_outs
from msr_last_inning mli
left join msr_total_outs mto
on mli.game_date = mto.game_date
and mli.pitcher = mto.pitcher
and mli.last_inning = mto.inning
),
aggregated as (
select
	game_date,
	pitcher,
	last_inning,
	last_inning_outs,
	case 
		when last_inning_outs = 3 then last_inning::float
		when last_inning_outs = 0 then last_inning::float - 1
		when last_inning_outs = 2 then (last_inning::float - 1) + (.667)
		when last_inning_outs = 1 then (last_inning::float - 1) + (.333)
	end as innings_pitched
	from msr_last_inning_and_outs
)
select * from aggregated