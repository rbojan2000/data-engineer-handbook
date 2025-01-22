WITH yesterday AS (
	SELECT * FROM actors
	WHERE current_year = 1999
),
today AS (
	SELECT 
	    actor,
	    actorid,
	    ARRAY_AGG(ROW(film, votes, rating, filmid, year)::film_stats) AS films,
	    avg(rating) AS average_rating,
		2000 AS year
	FROM actor_films
	WHERE year = 2000
	GROUP BY actor, actorid
)
INSERT INTO actors
SELECT
	COALESCE(t.actor, y.name) as name, 
	CASE
		WHEN y.films IS NULL THEN 
			t.films
		WHEN y.films IS NOT NULL THEN
			y.films || t.films
		ELSE
			y.films
	END AS films,
	CASE
		WHEN t.year IS NOT NULL THEN
			CASE
				WHEN t.average_rating > 8 THEN 'star'
				WHEN t.average_rating > 7 THEN 'good'
				WHEN t.average_rating > 6 THEN 'average'
				ELSE 'bad'
			END::quality_class
		ELSE y.quality_class
	END AS quality_class,
	(t.year is not null)::BOOLEAN AS is_active,
	CASE
		WHEN 
			t.year IS NOT NULL THEN t.year
 		ELSE
			y.current_year + 1
	END AS current_year
FROM today t 
FULL OUTER JOIN yesterday y
	ON t.actor = y.name