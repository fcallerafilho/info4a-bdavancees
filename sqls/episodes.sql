CREATE TABLE IF NOT EXISTS episodes(
        mid VARCHAR PRIMARY KEY,
        parentMid VARCHAR,
        seasonNumber INTEGER,
        episodeNumber INTEGER,
        FOREIGN KEY (parentMid) REFERENCES movies(mid)
    );