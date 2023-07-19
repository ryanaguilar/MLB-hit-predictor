with fct_pitch as (
    select * from {{ ref('fct_pitch') }}
),
counts as (
	select 
		game_date,
		pitcher,
		count(bb_type) as batted_balls_total
	from fct_pitch
	group by game_date, pitcher
),
aggregated as (
    select
        max(game_date),
        pitcher,
        sum(batted_balls_total)
    from counts
    group by pitcher
) 
select * from aggregated