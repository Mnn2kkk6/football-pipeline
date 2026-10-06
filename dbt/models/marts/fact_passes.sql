-- Sân StatsBomb: 120 x 80, khung thành đối phương ở x = 120.
with p as (
    select * from {{ ref('stg_events') }}
    where event_type = 'Pass' and pass_end_x is not null
)
select
    event_id                                   as pass_id,
    match_id, period, event_minute, event_second,
    team_id, team_name, player_id, player_name,
    pass_recipient_id                          as recipient_id,
    x                                          as start_x,
    y                                          as start_y,
    pass_end_x                                 as end_x,
    pass_end_y                                 as end_y,
    round(pass_length::numeric, 2)             as pass_length,
    round(pass_angle::numeric, 3)              as pass_angle,
    pass_height,
    coalesce(pass_outcome, 'Complete')         as pass_outcome,
    (pass_outcome is null)                     as is_complete,
    (pass_end_x - x >= 10)                     as is_progressive,   -- tiến >= 10 yard về phía khung thành
    (pass_end_x >= 102 and pass_end_y between 18 and 62) as into_final_third_box_zone,
    coalesce(under_pressure, false)            as under_pressure
from p
