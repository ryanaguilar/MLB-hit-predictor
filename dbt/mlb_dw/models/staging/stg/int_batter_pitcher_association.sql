with stage as (
    select * from {{ ref('stg_starting_pitchers') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(['batter','pitcher','game_date']) }} as k_batter_pitcher_date,
        {{ dbt_utils.generate_surrogate_key(['batter']) }} as k_batter,
        {{ dbt_utils.generate_surrogate_key(['pitcher']) }} as k_pitcher,
        {{ dbt_utils.generate_surrogate_key(['game_date']) }} as k_date,
        stage.game_date
    from stage
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_batter_pitcher_date',
            order_by='game_date'
            )
    }}
)
select * from deduped