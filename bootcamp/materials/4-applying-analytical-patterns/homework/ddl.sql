 CREATE table players_growth_accounting (
     player_name TEXT,
     first_active_season INT,
     last_active_season INT,
     state TEXT,
     seasons_active INT[],
     season INT,
     PRIMARY KEY (player_name, season)
 );
