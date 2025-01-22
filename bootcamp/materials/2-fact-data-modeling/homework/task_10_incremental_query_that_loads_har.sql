insert into host_activity_reduced
with daily_aggregate as (
	select
		host,
		date(event_time) as date,
		count(1) as num_site_hits,
	    ARRAY_AGG(user_id) AS unique_visitors
	from 
		events
	where 
		date(event_time) = date('2023-01-02') and
		host is not null and
		user_id is not Null
	group by
		host, date(event_time) 
),
yesterday_array as (
	select * from host_activity_reduced
	where month_start = Date('2023-01-01')
)	
select 
	coalesce(da.host, ya.host) as host,
	coalesce(ya.month_start, DATE_TRUNC('month', da.date)) as month_start,
	case 
		when 
			ya.hit_array is not null then
				ya.hit_array || ARRAY[coalesce(da.num_site_hits, 0)]
		when ya.month_start is null then
			ARRAY[coalesce(da.num_site_hits, 0)]
		when
			ya.hit_array is null 
			then array_fill(0, array[coalesce(date - DATE(date_trunc('month', date)), 0)]) || array[coalesce(da.num_site_hits, 0)]
		end as hit_array,
	case
		when ya.unique_visitors is null then
			da.unique_visitors	
		when da.unique_visitors is null then
			ya.unique_visitors	
		else
			ya.unique_visitors || da.unique_visitors
		end as unique_visitors
from daily_aggregate da
full outer join yesterday_array ya on
	da.host = ya.host
on conflict (host, month_start)
do 
	update set hit_array = EXCLUDED.hit_array;