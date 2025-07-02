select 
player_name,
current_season,
ROW_NUMBER() over (partition BY current_season)
from players;

with height_table as (
    SELECT DISTINCT ON (player_name) player_name, 
    (split_part(height, '-', 1)::int * 12) + split_part(height, '-', 2)::int as height, 
    country
    FROM PLAYERS
    WHERE height is not null
    ORDER BY player_name, current_season DESC
),
ranked_height as (
    SELECT 
    player_name,
    height, 
    country,
    dense_rank() over (PARTITION BY country order by height desc) as dr
    from height_table
)
SELECT * from ranked_height
WHERE dr <= 3;

Select distinct on (player_name) * from players
where country = 'Angola';


create table players_scd_table
(
	player_name text,
	scoring_class scoring_class,
	is_active boolean,
	start_season integer,
	end_date integer,
	current_season INTEGER
);

drop type if exists scd_type;
CREATE TYPE scd_type AS (
                    scoring_class scoring_class,
                    is_active boolean,
                    start_season INTEGER,
                    end_season INTEGER
);

with with_previous as (
    SELECT player_name, scoring_class,
    LAG(scoring_class) over (partition by player_name order by current_season) as prev_scoring_class,
    isactive,
    LAG(isactive) over (partition by player_name order by current_season) as prev_isactive,
    current_season
    FROM players
),
 with_indicator as (
    select *,
    CASE 
        WHEN scoring_class <> prev_scoring_class THEN 1
        WHEN isactive <> prev_isactive THEN 1
    END as change_indicator
    FROM with_previous
),
with_streaks as (
    SELECT *, 
        SUM(change_indicator) over (partition by player_name order by current_season) as streak_identifier
    from with_indicator
)
SELECT player_name, isactive, scoring_class,
    MIN(current_season) as start_season,
    MAX(current_season) as end_season
from with_streaks
group by player_name,isactive, scoring_class
order by player_name;