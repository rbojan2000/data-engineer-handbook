CREATE TYPE film_stats AS (
    film VARCHAR(255),
    votes INT,
    rating FLOAT,
    filmid UUID,
    year INT
);

CREATE TYPE quality_class AS ENUM (
    'star', 
    'good', 
    'average', 
    'bad'
);

CREATE TABLE actors (
    name TEXT NOT NULL,
    films film_stats[],
    quality_class quality_class,
    is_active BOOLEAN,
    current_year,
    PRIMARY KEY(name, current_year)
);