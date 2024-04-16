\c partitions;

CREATE TABLE by_timestamp (
    id              BIGSERIAL,
    temperature     INT,
    created_at      TIMESTAMP NOT NULL
) PARTITION BY RANGE (created_at);

CREATE TABLE by_timestamp_2000 PARTITION OF by_timestamp FOR VALUES FROM ('2000-01-01') TO ('2001-01-01');
CREATE TABLE by_timestamp_2001 PARTITION OF by_timestamp FOR VALUES FROM ('2001-01-01') TO ('2002-01-01');
CREATE TABLE by_timestamp_2002 PARTITION OF by_timestamp FOR VALUES FROM ('2002-01-01') TO ('2003-01-01');

INSERT INTO by_timestamp values (1, floor(RANDOM()*100), '2000-12-31T23:54:00');
INSERT INTO by_timestamp values (2, floor(RANDOM()*100), '2001-12-31T23:54:00');
INSERT INTO by_timestamp values (3, floor(RANDOM()*100), '2002-12-31T23:54:00');
