-- Phong độ cầu thủ theo từng trận (dùng cho dashboard).
select
    e.match_id,
    m.match_date,
    e.player_id,
    max(e.player_name)  as player_name,
    e.team_id,
    max(e.team_name)    as team_name,
    count(*) filter (where e.event_type = 'Pass')                                   as passes,
    count(*) filter (where e.event_type = 'Pass' and e.pass_outcome is null)        as passes_completed,
    round(100.0 * count(*) filter (where e.event_type = 'Pass' and e.pass_outcome is null)
          / nullif(count(*) filter (where e.event_type = 'Pass'), 0), 1)            as pass_accuracy_pct,
    count(*) filter (where e.event_type = 'Shot')                                   as shots,
    count(*) filter (where e.event_type = 'Shot' and e.shot_outcome = 'Goal')       as goals,
    round(coalesce(sum(e.shot_xg) filter (where e.event_type = 'Shot'), 0)::numeric, 2) as xg,
    count(*) filter (where e.event_type in
        ('Interception', 'Ball Recovery', 'Duel', 'Block', 'Clearance'))            as defensive_actions
from {{ ref('stg_events') }} e
left join {{ ref('stg_matches') }} m using (match_id)
group by e.match_id, m.match_date, e.player_id, e.team_id
