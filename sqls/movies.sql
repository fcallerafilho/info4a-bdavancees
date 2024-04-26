CREATE TABLE IF NOT EXISTS movies(
    mid VARCHAR PRIMARY KEY,
    titleType VARCHAR,
    primaryTitle VARCHAR,
    originalTitle VARCHAR,
    isAdult INTEGER CHECK (isAdult IN (0, 1)),
    startYear INTEGER,
    endYear INTEGER,
    runtimeMinutes INTEGER
);