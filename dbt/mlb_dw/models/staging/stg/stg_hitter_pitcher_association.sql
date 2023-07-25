with base_pitches as (
    select * from {{ ref('base_pitches') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(['batter','pitcher','game_date']) }} as k_batter_pitcher_date,
        {{ dbt_utils.generate_surrogate_key(['batter']) }} as k_batter,
        {{ dbt_utils.generate_surrogate_key(['pitcher']) }} as k_pitcher,
        {{ dbt_utils.generate_surrogate_key(['game_date']) }} as k_date,
        base_pitches.game_date,
        base_pitches.index
    from base_pitches
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_batter_pitcher_date',
            order_by='index'
            )
    }}
)
select * from deduped