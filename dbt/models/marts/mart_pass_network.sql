-- Mạng lưới chuyền bóng: node = vị trí chuyền trung bình, cạnh = số đường chuyền thành công.
with avg_pos as (
    select match_id, player_id, avg(start_x) as avg_x, avg(start_y) as avg_y, count(*) as passes_made
    from {{ ref('fact_passes') }}
    group by match_id, player_id
),
edges as (
    select match_id, team_id, player_id as passer_id, recipient_id, count(*) as pass_count
    from {{ ref('fact_passes') }}
    where is_complete and recipient_id is not null
    group by match_id, team_id, player_id, recipient_id
)
select
    e.match_id, e.team_id,
    e.passer_id,    pp.player_name as passer_name,
    e.recipient_id, rp.player_name as recipient_name,
    e.pass_count,
    round(a1.avg_x::numeric, 1) as passer_avg_x,    round(a1.avg_y::numeric, 1) as passer_avg_y,
    round(a2.avg_x::numeric, 1) as recipient_avg_x, round(a2.avg_y::numeric, 1) as recipient_avg_y
from edges e
join avg_pos a1 on a1.match_id = e.match_id and a1.player_id = e.passer_id
join avg_pos a2 on a2.match_id = e.match_id and a2.player_id = e.recipient_id
left join {{ ref('dim_players') }} pp on pp.player_id = e.passer_id
left join {{ ref('dim_players') }} rp on rp.player_id = e.recipient_id
