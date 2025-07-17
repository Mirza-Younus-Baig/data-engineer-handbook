create type vertex_type as enum('team', 'game', 'player');
create type edge_type as enum('plays_against', 'shares_team','plays_in', 'plays_on');


create table vertices(
    identifier TEXT,
    type vertex_type,
    properties JSON,
    PRIMARY KEY(identifier, type));

CREATE TABLE edges(
    subject_identifier TEXT,
    subject_type vertex_type,
    object_identifier TEXT,
    object_type vertex_type,
    edge_type edge_type,
    properties JSON,
    PRIMARY KEY(subject_identifier, subject_type, object_identifier, object_type, edge_type)
)

delete from vertices;
delete from edges;

INSERT INTO vertices(
SELECT 
    game_id as identifier, 
    'game'::vertex_type as type,
    json_build_object(
    'pts_home',pts_home,
    'pts_away',pts_away,
    'winning_team', CASE WHEN home_team_wins = 1 then home_team_id else visitor_team_id end) as properties
from games);


with deduped_teams as (
    SELECT *, ROW_NUMBER() over (partition by team_id) as row_number
    from teams
)
INSERT INTO vertices
select 
    team_id as identifier,
    'team'::vertex_type as type,
    json_build_object(
        'name', nickname,
        'year_founded', yearfounded,
        'city', city,
        'arena', arena,
        'owner', owner
    ) as properties
    from deduped_teams
    where row_number = 1;


with player_details as (
    SELECT 
        player_id as identifier,
        MAX(player_name) as player_name,
        count(1) as total_games,
        SUM(pts) as total_points,
        array_agg(DISTINCT team_id) as teams
        from game_details
        group by player_id
)
INSERT INTO vertices (
select 
    identifier,
    'player'::vertex_type as type,
    json_build_object(
        'player_name', player_name,
        'total_games', total_games,
        'total_points', total_points,
        'teams', teams
    )
from player_details
);

select type, count(1) from vertices
group by type;

select * from game_details;

with deduped_game_details as (
    select *, ROW_NUMBER() over (partition by player_id, game_id) as row_num
    from game_details
)
insert into edges(
select 
    player_id as subject_identifier,
    'player'::vertex_type as subject_type,
    game_id as object_identifier,
    'game'::vertex_type as object_type,
    'plays_in'::edge_type as edge_type,
    json_build_object(
        'player_name', player_name,
        'team_abbreviation', team_abbreviation,
        'start_position', start_position,
        'points', pts
    )
from deduped_game_details
where row_num = 1
);

select * from edges
where subject_identifier='1713';


SELECT
    v.properties->>'player_name' as name,
    sum((e.properties->>'points')::INTEGER) as points
From vertices v join edges e
ON v.identifier = e.subject_identifier
AND v.type = e.subject_type
where e.properties->>'points' is not NULL
group by 1
order by 2 desc, 1;


select game_id, player_id, team_id, count(1) as cnt from game_details
group by game_id, player_id, team_id
order by cnt desc;


