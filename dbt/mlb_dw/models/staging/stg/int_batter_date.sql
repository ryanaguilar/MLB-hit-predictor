with events as (
    select * from {{ ref('stg_batter_events') }}
),
agg as (
    select
        k_batter,
        game_date,
        percentile_cont(0.5) within group (order by launch_angle) as la_median
    from events
    group by k_batter, game_date
)
select * from agg