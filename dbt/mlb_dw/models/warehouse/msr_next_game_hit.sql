with fct_pitch as (
    select * from {{ ref('fct_pitch') }}
),
msr_hit_indicator as (
	select 
		game_date,
		pitcher,
		player_name,
		case 
			when events in ('single', 'double', 'triple', 'home_run') then 1
			else 0
		end as hit_counter
	from fct_pitch fp 
	--,player_name 
),
msr_hits as (
	select 
		game_date,
		pitcher,
		player_name,
		sum(hit_counter) as hits
	from msr_hit_indicator
	group by game_date, pitcher, player_name 
	--,player_name 
),
next_game_hit as (
    select
        game_date,
        pitcher,
        player_name,
        case
            when hits > 0 then 1
            else 0
        end as next_game_HIT
    from msr_hits
)
select * from next_game_hit