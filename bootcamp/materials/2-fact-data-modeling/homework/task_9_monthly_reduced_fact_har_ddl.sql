create table host_activity_reduced (
    host TEXT,
	month DATE,
    hit_array INT[],
	unique_visitors NUMERIC[],
	primary key (host, month)	
)
