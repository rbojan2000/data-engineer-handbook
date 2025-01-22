create table hosts_cumulated (
	host TEXT,
	month_start DATE,
	metric_name TEXT,
	host_activity_datelist INT[],
	primary key (host, month_start, metric_name)
)