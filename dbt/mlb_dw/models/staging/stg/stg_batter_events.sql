with events as (
    select * from {{ ref('base_batter_events') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(['batter']) }} as k_batter,
        {{ dbt_utils.generate_surrogate_key(['pitcher']) }} as k_pitcher,
        {{ dbt_utils.generate_surrogate_key(['game_date']) }} as k_date,
        {{ dbt_utils.generate_surrogate_key(['batter','pitcher','game_date','index']) }} as k_event,
        events.*
    from events
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_event',
            order_by='index'
        )
    }}
)
select * from deduped