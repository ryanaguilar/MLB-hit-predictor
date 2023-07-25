with boxes as (
  select * from {{ source('mlb_hp', 'box_scores_pitchers') }}
),
renamed as (
select 
    game_date,
	game as game,
	player as player,
	(v_season_stats ->> 'gamesPlayed')::int as games_played,
	(v_season_stats ->> 'flyOuts')::int as sz_fly_outs,
	(v_season_stats ->> 'groundOuts')::int as sz_ground_outs,
	(v_season_stats ->> 'airOuts')::int as sz_air_outs,
    (v_season_stats ->> 'runs')::int as sz_runs,
    (v_season_stats ->> 'doubles')::int as sz_doubles,
    (v_season_stats ->> 'triples')::int as sz_triples,
	(v_season_stats ->> 'homeRuns')::int as sz_home_runs,
    (v_season_stats ->> 'strikeOuts')::int as sz_strike_outs,
	(v_season_stats ->> 'baseOnBalls')::int as sz_bb,
	(v_season_stats ->> 'intentionalWalks')::int as sz_ibb,
	(v_season_stats ->> 'hits')::int as sz_hits,
    (v_season_stats ->> 'hiByPitch')::int as sz_hit_by_pitch,
	(v_season_stats ->> 'atBats')::int as sz_at_bats,
    (v_season_stats ->> 'caughtStealing')::int as sz_caught_stealing,
    (v_season_stats ->> 'stolenBases')::int as sz_stolen_bases,
    case
        when ((v_season_stats ->> 'stolenBasePercentage')::varchar) = '.---' then '.000'
        else (v_season_stats ->> 'stolenBasePercentage')::varchar
    end::float as sz_babip,
    (v_season_stats ->> 'numberOfPitches')::int as sz_number_of_pitches,
    (v_season_stats ->> 'inningsPitched')::float as sz_innings_pitched,
    (v_season_stats ->> 'pitchesThrown')::int as sz_pitches_thrown,
    (v_season_stats ->> 'balls')::int as sz_balls,
    (v_season_stats ->> 'strikes')::int as sz_strikes,
	(v_stats ->> 'flyOuts')::int as fly_outs,
	(v_stats ->> 'groundOuts')::int as ground_outs,
	(v_stats ->> 'airOuts')::int as air_outs,
    (v_stats ->> 'runs')::int as runs,
    (v_stats ->> 'doubles')::int as doubles,
    (v_stats ->> 'triples')::int as triples,
	(v_stats ->> 'homeRuns')::int as home_runs,
    (v_stats ->> 'strikeOuts')::int as strike_outs,
	(v_stats ->> 'baseOnBalls')::int as bb,
	(v_stats ->> 'intentionalWalks')::int as ibb,
	(v_stats ->> 'hits')::int as hits,
    (v_stats ->> 'hiByPitch')::int as hit_by_pitch,
	(v_stats ->> 'atBats')::int as at_bats,
    (v_stats ->> 'caughtStealing')::int as caught_stealing,
    (v_stats ->> 'stolenBases')::int as stolen_bases,
    case
        when ((v_stats ->> 'stolenBasePercentage')::varchar) = '.---' then '.000'
        else (v_stats ->> 'stolenBasePercentage')::varchar
    end::float as babip,
    (v_stats ->> 'numberOfPitches')::int as number_of_pitches,
    (v_stats ->> 'inningsPitched')::float as innings_pitched,
    (v_stats ->> 'pitchesThrown')::int as pitches_thrown,
    (v_stats ->> 'balls')::int as balls,
    (v_stats ->> 'strikes')::int as strikes,
    (v_stats ->> 'battersFaced')::int as batters_faced
from boxes
)
select * from renamed