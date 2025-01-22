
WITH agg AS (
    SELECT metric_name, month_start, ARRAY[SUM(host_activity_datelist[1]), SUM(host_activity_datelist[2])] AS summed_array
    FROM hosts_cumulated
    GROUP BY metric_name, month_start
)
SELECT 
    metric_name, 
     month_start + CAST(CAST(index - 1 AS TEXT) || ' day' AS INTERVAL) AS adjusted_date,
    elem AS value
FROM agg
CROSS JOIN UNNEST(agg.summed_array) WITH ORDINALITY AS a(elem, index);
