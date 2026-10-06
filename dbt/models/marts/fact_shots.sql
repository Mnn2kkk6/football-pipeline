-- Khung thành: x = 120, y từ 36 đến 44 (rộng 8 yard, tâm y = 40).
-- Góc sút = góc nhìn tới hai cột dọc: atan2(8*dx, dx^2 + dy^2 - 16).
with s as (
    select * from {{ ref('stg_events') }} where event_type = 'Shot'
)
select
    event_id                                   as shot_id,
    match_id, period, event_minute, event_second,
    team_id, team_name, player_id, player_name,
    x                                          as shot_x,
    y                                          as shot_y,
    shot_end_x, shot_end_y,
    round(sqrt(power(120 - x, 2) + power(y - 40, 2))::numeric, 2) as distance_to_goal,
    round(degrees(atan2(8 * (120 - x),
                        power(120 - x, 2) + power(y - 40, 2) - 16))::numeric, 2) as shot_angle_deg,
    shot_xg,
    shot_outcome,
    (shot_outcome = 'Goal')                    as is_goal,
    shot_body_part, shot_technique, shot_type, shot_first_time,
    coalesce(under_pressure, false)            as under_pressure
from s
