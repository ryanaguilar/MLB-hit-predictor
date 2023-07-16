with subset as (
	select
		*
	from
		mlb_dw.raw.statcast_plate_appearances spa 
	where 
		game_date = '2023-07-06'
),
PA as (
select 
	game_date, 
	batter, 
	count(events) over (partition by game_date, batter order by game_date, batter) as hit_distance_sc 
from 
	subset
)
select * from PA
group by 1,2,3;

select 

select distinct hit_location from mlb_dw.raw.game_data_2023 gd ;

select distinct stand from mlb_dw.raw.game_data_2023 gd ;

select distinct bb_type from mlb_dw.raw.game_data_2023 gd ;

with renamed as (
select 
	game as game,
	player as player,
	game_date,
	(v_season_stats ->> 'gamesPlayed')::int as games_played,
	(v_season_stats ->> 'runs')::int as runs,
	(v_season_stats ->> 'doubles')::int as doubles,
	(v_season_stats ->> 'triples')::int as triples,
	(v_season_stats ->> 'homeRuns')::int as home_runs,
	(v_season_stats ->> 'baseOnBalls')::int as bb,
	(v_season_stats ->> 'intentionalWalks')::int as ibb,
	(v_season_stats ->> 'hits')::int as hits,
	(v_season_stats ->> 'avg')::varchar as avg,
	(v_season_stats ->> 'atBats')::int as at_bats,
	(v_season_stats ->> 'obp')::varchar as obp,
	(v_season_stats ->> 'slg')::varchar as slg,
	(v_season_stats ->> 'ops')::varchar as ops,
	(v_season_stats ->> 'plateAppearances')::int as plate_appearances,
	(v_season_stats -> 'babip')::varchar as babip
from mlb_dw.raw.box_scores bs  
limit 100
)
select * from renamed;

select * from raw_stage.base_box_scores bbs limit 100;

select * from raw_stage.stg_player_game_associations order by game_date, game limit 100;

select * from raw.box_scores_deprecated bsd where date = '2023-03-30' limit 100;

--create table raw.box_scores (game_date date, game int, player int, v_stats json, v_season_stats json);

select count(*) from raw.box_scores_deprecated bsd ;

select count(*) from raw.box_scores bs ;

select 
	k_player_game, 
	game_date, 
	hits,
	lead(hits) over (partition by k_player order by game_date) as next_game_hits
from 
	raw_stage.stg_player_game_associations where k_player = '53db2f55b90e015a818999dd642908b3' order by game_date, game limit 100;

select 
	k_player, 
	game_date, 
	hits,
	next_game_hit
from 
	raw_analytics.fct_player_game_association fpga  where k_player = '53db2f55b90e015a818999dd642908b3' order by game_date limit 100;

