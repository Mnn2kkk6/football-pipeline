select
    player_id,
    max(player_name)                                        as player_name,
    max(player_nickname)                                    as player_nickname,
    max(country_name)                                       as country_name,
    (array_agg(team_id      order by match_id desc))[1]     as team_id,
    (array_agg(team_name    order by match_id desc))[1]     as team_name,
    (array_agg(jersey_number order by match_id desc))[1]    as jersey_number,
    count(distinct match_id)                                as matches_in_squad
from {{ source('raw', 'players') }}
group by player_id
