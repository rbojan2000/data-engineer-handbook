create table user_devices_cumulated (
	user_id TEXT,
    device_activity_datelist JSONB,
	date DATE,
	primary key (user_id, date)	
)
