CREATE TABLE IF NOT EXISTS characters(
        mid VARCHAR SECONDARY KEY,
        pid VARCHAR SECONDARY KEY,
        name VARCHAR,
        FOREIGN KEY (mid) REFERENCES movies(mid),
        FOREIGN KEY (pid) REFERENCES persons(pid),
        PRIMARY KEY(mid,pid,name)
);