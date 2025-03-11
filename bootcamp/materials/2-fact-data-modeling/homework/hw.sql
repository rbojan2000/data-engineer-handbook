
--
--- A DDL for an user_devices_cumulated table that has:
--  - a device_activity_datelist which tracks a users active days by browser_type
--  - data type here should look similar to MAP<STRING, ARRAY[DATE]>
--    - or you could have browser_type as a column with multiple rows for each user (either way works, just be consistent!)
--
--- A cumulative query to generate device_activity_datelist from events
--
--- A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column 


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