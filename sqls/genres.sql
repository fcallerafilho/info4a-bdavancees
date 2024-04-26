CREATE TABLE IF NOT EXISTS genres(
    mid VARCHAR SECONDARY KEY,
    genre VARCHAR,
    FOREIGN KEY (mid) REFERENCES movies(mid),
    PRIMARY KEY(mid,genre)
);
