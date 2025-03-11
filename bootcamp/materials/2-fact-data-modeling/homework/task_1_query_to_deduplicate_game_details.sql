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
