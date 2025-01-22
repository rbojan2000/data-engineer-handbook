insert into hosts_cumulated
with daily_aggregate as (
	select
		host,
		date(event_time) as date,
		count(1) as num_site_hits
	from 
		events
	where 
		date(event_time) = date('2023-01-01') and
		host is not null	
	group by
		host, date(event_time) 
),
yesterday_array as (
	 select * from hosts_cumulated
	where month_start = Date('2023-01-01')
)	
select 
	coalesce(da.host, ya.host) as host,
	coalesce(ya.month_start, DATE_TRUNC('month', da.date)) as month_start,
	'site_hits' as metric_name,
	case 
		when 
			ya.host_activity_datelist is not null then
				ya.host_activity_datelist || ARRAY[coalesce(da.num_site_hits, 0)]
		when ya.month_start is null then
			ARRAY[coalesce(da.num_site_hits, 0)]
		when
			ya.host_activity_datelist is null 
			then array_fill(0, array[coalesce(date - DATE(date_trunc('month', date)), 0)]) || array[coalesce(da.num_site_hits, 0)]
		end as host_activity_datelist
from daily_aggregate da
full outer join yesterday_array ya on
	da.host = ya.host
on conflict (host, month_start, metric_name)
do 
	update set host_activity_datelist = EXCLUDED.host_activity_datelist;
