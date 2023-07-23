with int_pitcher_game as (
    select * from {{ ref('int_pitcher_agg_wide') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(['pitcher']) }} as k_pitcher,
        {{ dbt_utils.generate_surrogate_key(['pitcher', 'game_date']) }} as k_pitcher_date,
        int_pitcher_game.*
    from int_pitcher_game
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_pitcher_date',
            order_by='pitcher'
        )
    }}
)
select * from deduped