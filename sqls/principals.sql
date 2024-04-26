CREATE TABLE IF NOT EXISTS principals   (
    mid VARCHAR SECONDARY KEY,
    ordering INTEGER,
    pid VARCHAR SECONDARY KEY,
    category VARCHAR,
    job VARCHAR,
    FOREIGN KEY (pid) REFERENCES persons(pid),
    FOREIGN KEY (mid) REFERENCES movies(mid)
);