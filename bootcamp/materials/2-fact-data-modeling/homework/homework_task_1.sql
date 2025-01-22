--
--- A DDL for an `user_devices_cumulated` table that has:
--  - a `device_activity_datelist` which tracks a users active days by `browser_type`
--  - data type here should look similar to `MAP<STRING, ARRAY[DATE]>`
--    - or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)
--
--- A cumulative query to generate `device_activity_datelist` from `events`
--
--- A `datelist_int` generation query. Convert the `device_activity_datelist` column into a `datelist_int` column 

--drop table user_devices_cumulated 

--create table user_devices_cumulated (
--	user_id TEXT,
--    device_activity_datelist JSONB,
--	date DATE,
--	primary key (user_id, date)	
--)

with yesterday as (
	select *
	from user_devices_cumulated
	where date = DATE('2023-01-01')
),
today as (
	select
		cast(user_id as text) as user_id,
		date(cast(event_time as timestamp)) as date_active,
		device_type
	from 
	    devices 
	    left join events on devices.device_id = events.device_id 
	where
		date(cast(event_time as timestamp)) = date('2023-01-02') and 
		user_id is not null and
		device_type is not null
)
select
	COALESCE(t.user_id, y.user_id) as user_id

from 
	yesterday y
	full outer join today t on t.user_id= y.user_id


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
--
--select * from users_cumulated 