-- Tỷ lệ kiểm soát bóng thực tế, tính theo tỷ trọng số đường chuyền của mỗi đội trong trận.
select
    match_id,
    team_id,
    max(team_name)                                                   as team_name,
    count(*)                                                         as passes,
    round(100.0 * count(*) / sum(count(*)) over (partition by match_id), 1) as possession_pct
from {{ ref('fact_passes') }}
group by match_id, team_id
