Select * from player_seasons limit 10;

drop type if exists season_stats cascade;
drop type if exists scoring_class cascade;

create type season_stats as (
	season Integer,
	gp Integer,
	pts Real,
	reb Real,
	ast Real,
    weight Integer
);

create type scoring_class as ENUM(
    'star',
    'good',
    'average',
    'bad'
);

drop table if exists players;
create table if not exists players(
    player_name text,
    height text,
    college text,
    country text,
    draft_year text,
    draft_round text,
    draft_number text,
    seasons season_stats[],
    scoring_class scoring_class,
    years_since_last_active integer,
    isActive Boolean,
    current_season Integer,
    PRIMARY KEY (player_name, current_season)
);

-- SELECT * FROM pg_tables where schemaname='public';
select * from players; 
delete from players;

DO $$ 
DECLARE 
    minYear int;
    maxYear int;
BEGIN

SELECT min(season), max(season) 
INTO minYear, maxYear FROM player_seasons;

INSERT INTO players
SELECT 
    player_name,
    height,
    college,
    country,
    draft_year,
    draft_round,
    draft_number,
    ARRAY[ROW(
        season, gp, pts, reb, ast, weight)::season_stats] AS seasons,
    (CASE 
        WHEN pts > 20 THEN 'star'
        WHEN pts > 15 THEN 'good'
        WHEN pts > 10 THEN 'average'
        ELSE 'bad' 
    END)::scoring_class as scoring_class,
    0 AS years_since_last_active,
    true AS is_active,
    season AS current_season
FROM player_seasons
WHERE season = minYear;

FOR i IN minYear+1..maxYear LOOP
WITH last_season AS (
    SELECT * FROM players
    WHERE current_season = i - 1
), this_season AS (
     SELECT * FROM player_seasons
    WHERE season = i
)
INSERT INTO players
SELECT
    COALESCE(ls.player_name, ts.player_name) as player_name,
    COALESCE(ls.height, ts.height) as height,
    COALESCE(ls.college, ts.college) as college,
    COALESCE(ls.country, ts.country) as country,
    COALESCE(ls.draft_year, ts.draft_year) as draft_year,
    COALESCE(ls.draft_round, ts.draft_round) as draft_round,
    COALESCE(ls.draft_number, ts.draft_number) as draft_number,
    COALESCE(ls.seasons, ARRAY[]::season_stats[]) || 
        CASE 
            WHEN ts.season IS NOT NULL THEN
                ARRAY[ROW(
                    ts.season, 
                    ts.gp, 
                    ts.pts, 
                    ts.reb, 
                    ts.ast, 
                    ts.weight
                )::season_stats]
            ELSE 
                ARRAY[]::season_stats[]
        END
        AS seasons,
    CASE 
        WHEN ts.season is not null THEN
        (CASE 
            WHEN ts.pts > 20 THEN 'star'
            WHEN ts.pts > 15 THEN 'good'
            WHEN ts.pts > 10 THEN 'average'
            ELSE 'bad' END)::scoring_class
        ELSE ls.scoring_class
    END as scoring_class,
    CASE 
        WHEN ts.season is not null then 0
        ELSE COALESCE(ls.years_since_last_active, 0) + 1
    END as years_since_last_active,
    ts.season is not null as is_active,
    COALESCE(ts.season, ls.current_season + 1) as current_season
    FROM last_season ls
    FULL OUTER JOIN this_season as ts
    ON ls.player_name = ts.player_name;

END LOOP;
END $$;

SELECT * FROM PLAYERS;
SELECT COUNT(*) FROM PLAYERS;
SELECT COUNT(DISTINCT player_name) from players;
SELECT * FROM PLAYERS WHERE player_name = 'Michael Jordan';
SELECT player_name, current_season, (unnest(seasons)::season_stats).* from players order by player_name;
SELECT player_name, seasons[1] as first_season, seasons[cardinality(seasons)] as last_season from players;
