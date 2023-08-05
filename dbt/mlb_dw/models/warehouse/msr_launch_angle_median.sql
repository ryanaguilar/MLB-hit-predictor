with events as (
    select * from {{ ref('stg_batter_events') }} 
),
launch_angles_ordered as (
    select
    	index,
        k_batter,
        game_date,
        --trim(both '"' from launch_angle)::int as launch_angle
        launch_angle::numeric::int
    from events
    where launch_angle is not null
    	and events is not null 
   	order by k_batter, game_date asc, index asc
),
arrayed as (
select
	index,
	k_batter,
	game_date,
	launch_angle,
	array_agg(launch_angle) over (partition by k_batter order by game_date, index) as launch_angle_array
from launch_angles_ordered
),
unnested as (
select 
	index,
	k_batter,
	game_date,
	launch_angle_array,
	unnest(launch_angle_array) as sorted_int
from arrayed 
),
max_index as (
select
	*,
	max(index) over (partition by k_batter, game_date) as max_index
from unnested
),
cumulative_angles as (
select
	*
from max_index
where index = max_index
),
la_median as (
select 
	index, 
	k_batter, 
	game_date, 
	launch_angle_array, 
	max_index,
	percentile_cont(0.5) within group(order by sorted_int) as la_median
from cumulative_angles
group by index, k_batter, game_date, launch_angle_array, max_index
order by 
	k_batter,
	game_date
)
select * from la_median