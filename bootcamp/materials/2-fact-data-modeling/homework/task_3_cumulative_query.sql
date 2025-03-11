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
