-- A DDL for an user_devices_cumulated table that has:
--  - a device_activity_datelist which tracks a users active days by browser_type
--  - data type here should look similar to MAP<STRING, ARRAY[DATE]>
--    - or you could have browser_type as a column with multiple rows for each user (either way works, just be consistent!)
--
--- A cumulative query to generate device_activity_datelist from events
--
--- A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column 

--drop table user_devices_cumulated 

--create table user_devices_cumulated (
--	user_id TEXT,
--  device_activity_datelist JSONB,
--	date DATE,
--	primary key (user_id, date)	
--)

--DROP DATABASE tsdb;

--insert into users_cumulated
--with yesterday as (
--	select *
--	from users_cumulated  
--	where date = DATE('2023-01-03')
--),
--today as (
--	select 
--		cast(user_id as text) as t_user_id,
--		date(cast(event_time as timestamp)) as date_active	
--	from events
--	where 
--		date(cast(event_time as timestamp)) = date('2023-01-04') and 
--		user_id is not null
--	group by user_id, date(cast(event_time as timestamp))
--)
--SELECT
--       COALESCE(t.t_user_id, y.user_id) as user_id,
--       COALESCE(y.dates_active,
--           ARRAY[]::DATE[])
--            || CASE WHEN
--                t.t_user_id IS NOT NULL
--                THEN ARRAY[t.date_active]
--                ELSE ARRAY[]::DATE[]
--                END AS date_list,
--       COALESCE(t.date_active, y.date + Interval '1 day') as date
--from yesterday y
--full outer join today t
--on t.t_user_id= y.user_id


with deduped as (
	select 
		gd.*,
		g.game_date_est,
		g.season,
		g.home_team_id,
		g.visitor_team_id,
		ROW_NUMBER() over (partition by gd.game_id, team_id, player_id order by g.game_date_est	) as row_num
	from game_details gd
	join games g on gd.game_id = g.game_id
)
select * from deduped
where row_num = 1

with today AS (
    SELECT
        CAST(user_id AS TEXT) AS user_id,
        DATE(CAST(event_time AS TIMESTAMP)) AS date_active,
        ARRAY_AGG(DISTINCT device_type) AS device_types
    FROM 
        devices 
        LEFT JOIN events ON devices.device_id = events.device_id 
    WHERE
        DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-04')
        AND user_id IS NOT NULL
        AND device_type IS NOT NULL
    GROUP BY 
        user_id, DATE(CAST(event_time AS TIMESTAMP))
)

select * from user_devices_cumulated 
delete from user_devices_cumulated

insert into user_devices_cumulated
WITH yesterday AS (
    SELECT *
    FROM user_devices_cumulated
    WHERE date = DATE('2023-01-04')
),
today AS (
    SELECT
        CAST(user_id AS TEXT) AS user_id,
        DATE(CAST(event_time AS TIMESTAMP)) AS date_active,
        ARRAY_AGG(DISTINCT device_type) AS device_types
    FROM 
        devices 
        LEFT JOIN events ON devices.device_id = events.device_id 
    WHERE
        DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-05')
        AND user_id IS NOT NULL
        AND device_type IS NOT NULL
    GROUP BY 
        user_id, DATE(CAST(event_time AS TIMESTAMP))
)
SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(y.device_activity_datelist, '{}'::JSONB) || 
	    CASE 
	        WHEN t.date_active IS NOT NULL THEN
	            (
	                SELECT JSONB_OBJECT_AGG(
	                    device_type,
	                    COALESCE(
	                        (y.device_activity_datelist -> device_type)::JSONB || TO_JSONB(ARRAY[t.date_active]),
	                        TO_JSONB(ARRAY[t.date_active])
	                    )
	                )
	                FROM UNNEST(t.device_types) AS device_type
	            )
	        ELSE 
	            '{}'::JSONB
	    END AS device_activity_datelist,
    COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM 
    yesterday y
FULL OUTER JOIN today t 
    ON t.user_id = y.user_id;


WITH users AS (
    SELECT * 
    FROM user_devices_cumulated
    WHERE date = DATE('2023-01-04')
),
series AS (
    SELECT generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') AS series_date
),
place_hoder_ints as (
	select
		cast(case when 
		    EXISTS (
		        SELECT 1
		        FROM jsonb_each(users.device_activity_datelist) AS each_device(device_type, dates_array)
		        WHERE dates_array @> TO_JSONB(ARRAY[series.series_date::DATE])
		    )
		 then cast(POW(2, 32 - (date - DATE(series_date))) as bigint)
		 else 0
		 end as  bit(32)) as placeholder_int_value,
	     users.device_activity_datelist,
	     series.series_date,
		* 	 
	FROM 
	    users
	CROSS JOIN 
	    series 
)
select * from place_hoder_ints