with boxes as (
  select * from {{ source('mlb_hp', 'box_scores') }}
),
renamed as (
select 
    game_date,
	game as game,
	player as player,
	(v_season_stats ->> 'gamesPlayed')::int as games_played,
	(v_season_stats ->> 'runs')::int as sz_runs,
	(v_season_stats ->> 'doubles')::int as sz_doubles,
	(v_season_stats ->> 'triples')::int as sz_triples,
	(v_season_stats ->> 'homeRuns')::int as sz_home_runs,
	(v_season_stats ->> 'baseOnBalls')::int as sz_bb,
	(v_season_stats ->> 'intentionalWalks')::int as sz_ibb,
	(v_season_stats ->> 'hits')::int as sz_hits,
	(v_season_stats ->> 'avg')::float as sz_avg,
	(v_season_stats ->> 'atBats')::int as sz_at_bats,
	(v_season_stats ->> 'obp')::float as sz_obp,
	(v_season_stats ->> 'slg')::float as sz_slg,
	(v_season_stats ->> 'ops')::float as sz_ops,
	(v_season_stats ->> 'plateAppearances')::int as sz_plate_appearances,
    (v_season_stats ->> 'sacFlies')::int as sz_sac_flies,
    (v_season_stats ->> 'strikeOuts')::int as sz_strike_outs,
    (v_stats ->> 'hits')::int as current_game_hits,
	case
        when ((v_season_stats ->> 'babip')::varchar) = '.---' then '.000'
        else (v_season_stats ->> 'babip')::varchar
    end::float as sz_babip
from boxes
)
select * from renamed