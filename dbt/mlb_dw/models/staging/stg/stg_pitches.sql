with base_pitches as (
    select * from {{ ref('base_pitches') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(['pitcher', 'game_date']) }} as k_pitcher_date,
        {{ dbt_utils.generate_surrogate_key(['pitcher', 'index', 'game_date']) }} as k_pitch,
        base_pitches.*
    from base_pitches
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_pitch',
            order_by='game_date'
            )
    }}
)
select * from deduped