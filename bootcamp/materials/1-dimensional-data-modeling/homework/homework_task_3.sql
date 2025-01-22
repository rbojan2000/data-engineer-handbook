CREATE TABLE actors_scd (
    name TEXT,
    quality_class quality_class,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER,
    current_year INTEGER,
    PRIMARY KEY(name, start_date)
);
