with base_boxes as (
    select * from {{ ref('base_box_scores_pitchers') }}
    where games_played <> '0'
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(['player']) }} as k_pitcher,
        {{ dbt_utils.generate_surrogate_key(['game']) }} as k_game,
        {{ dbt_utils.generate_surrogate_key(['player', 'game']) }} as k_pitcher_game,
        {{ dbt_utils.generate_surrogate_key(['player', 'game_date']) }} as k_pitcher_date,
        base_boxes.*
    from base_boxes
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_pitcher_date',
            order_by='player'
        )
    }}
)
select * from deduped