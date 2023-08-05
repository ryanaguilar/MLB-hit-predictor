with events as (
    select * from {{ ref('stg_batter_events') }}
),
counts as (
	select 
		game_date,
		k_batter,
        count(batter) filter (where events in ('single', 'double', 'triple', 'home_run')) as hits
	from events
	group by game_date, k_batter
),
aggregated as (
    select
        game_date,
        k_batter,
        sum(hits) over (between unbounded and current row) as hits
    from counts
    group by game_date, k_batter
) 
select * from aggregated