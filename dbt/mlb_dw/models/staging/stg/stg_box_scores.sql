with base_boxes as (
    select * from {{ ref('base_box_scores') }}
    where games_played <> '0'
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(['player']) }} as k_batter,
        {{ dbt_utils.generate_surrogate_key(['game']) }} as k_game,
        {{ dbt_utils.generate_surrogate_key(['player', 'game']) }} as k_batter_game,
        {{ dbt_utils.generate_surrogate_key(['player', 'game_date']) }} as k_batter_date,
        base_boxes.*
    from base_boxes
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_batter_date',
            order_by='player'
        )
    }}
)
select * from deduped