CREATE TABLE IF NOT EXISTS directors(
        mid VARCHAR SECONDARY KEY,
        pid VARCHAR SECONDARY KEY,  
        FOREIGN KEY (mid) REFERENCES movies(mid),
        FOREIGN KEY (pid) REFERENCES persons(pid),
        PRIMARY KEY(mid,pid)
    );