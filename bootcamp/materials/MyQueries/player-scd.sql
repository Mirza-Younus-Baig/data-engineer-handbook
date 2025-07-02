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



with with_previous as (

    SELECT player_name, scoring_class,
    LAG(scoring_class) over (partition by player_name order by current_season) as prev_scoring_class,
    isactive,
    LAG(isactive) over (partition by player_name order by current_season) as prev_isactive
    FROM players
),
 with_indicator as (
    select *,
    CASE 
        WHEN scoring_class <> prev_scoring_class THEN 1
        ELSE 0
    END as has_changed_scoring_class,
    CASE 
        WHEN isactive <> prev_isactive THEN 1
        ELSE 0
    END as has_changed_isactive
    FROM with_previous
)
with streak
SELECT * FROM with_indicator where has_changed_scoring_class = 1 or has_changed_isactive = 1;