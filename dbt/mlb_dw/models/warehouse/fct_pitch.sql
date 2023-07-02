with stg_pitches as (
    select * from {{ ref('stg_pitches') }}
)
select
     * 
from stg_pitches