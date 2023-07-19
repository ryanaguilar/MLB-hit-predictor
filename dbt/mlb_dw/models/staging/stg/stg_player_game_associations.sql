with base_boxes as (
    select * from {{ ref('base_box_scores') }}
    where games_played <> '0'
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(['player']) }} as k_player,
        {{ dbt_utils.generate_surrogate_key(['game']) }} as k_game,
        {{ dbt_utils.generate_surrogate_key(['player', 'game']) }} as k_player_game,
        base_boxes.*
    from base_boxes
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_player_game',
            order_by='game_date'
        )
    }}
)
select * from deduped