CREATE TABLE IF NOT EXISTS ratings   (
    mid VARCHAR SECONDARY KEY,
    averageRating REAL,
    numVotes INTEGER,
    FOREIGN KEY (mid) REFERENCES movies(mid)
);