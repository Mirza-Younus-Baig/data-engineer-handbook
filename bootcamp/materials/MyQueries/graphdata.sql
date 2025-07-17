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


WITH deduped_game_details AS
  (
         SELECT *,
                row_number() over (partition by player_id, game_id) AS row_num
         FROM   game_details ), 
    filtered_game_details AS
  (
         SELECT *
         FROM   deduped_game_details
         WHERE  row_num = 1 ), 
    aggregated AS
  (
           SELECT   g1.player_id         AS subject_identifier,
                    max(g1.player_name)  AS subject_player_name,
                    g2.player_id         AS object_identifier,
                    max(g2.player_name)  AS object_player_name,
                    g1.team_abbreviation AS subject_player_team,
                    g2.team_abbreviation AS object_player_team,
                    CASE
                             WHEN g1.team_abbreviation = g2.team_abbreviation THEN 'shares_team' :: edge_type
                             ELSE 'plays_against' :: edge_type
                    end         AS edge_type,
                    array_agg(g1.game_id) as games,
                    count(1)    AS no_of_games,
                    sum(g1.pts) AS subject_points,
                    sum(g2.pts) AS object_points
           FROM     filtered_game_details g1
           JOIN     filtered_game_details g2
           ON       g1.game_id = g2.game_id
           AND      g1.player_id <> g2.player_id
           GROUP BY 1,
                    3,
                    5,
                    6)
    INSERT INTO edges (
    SELECT 
        subject_identifier,
        'player'::vertex_type AS subject_type,
        object_identifier,
        'player'::vertex_type AS object_type,
        edge_type,
        json_build_object( 'subject_player_name', subject_player_name, 'object_player_name', object_player_name, 'subject_player_team', subject_player_team, 'object_player_team',object_player_team, 'total_games', no_of_games, 'subject_points', subject_points, 'object_points', object_points )
    FROM (
        SELECT DISTINCT *
        FROM aggregated
    ) dd );
