CREATE TABLE IF NOT EXISTS titles   (
    mid VARCHAR SECONDARY KEY,
    ordering INTEGER,
    title VARCHAR,
    region VARCHAR,
    language VARCHAR,
    types VARCHAR,
    attributes VARCHAR,
    isOriginalTitle INTEGER CHECK (isOriginalTitle IN (0, 1)),           
    FOREIGN KEY (mid) REFERENCES movies(mid)
);