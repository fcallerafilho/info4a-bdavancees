CREATE TABLE IF NOT EXISTS writers   (
    mid VARCHAR SECONDARY KEY,
    pid VARCHAR SECONDARY KEY,          
    FOREIGN KEY (mid) REFERENCES movies(mid)
    FOREIGN KEY (pid) REFERENCES persons(pid)  
);