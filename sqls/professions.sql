CREATE TABLE IF NOT EXISTS professions   (
    pid VARCHAR SECONDARY KEY,
    jobName VARCHAR,
    FOREIGN KEY (pid) REFERENCES persons(pid)
);