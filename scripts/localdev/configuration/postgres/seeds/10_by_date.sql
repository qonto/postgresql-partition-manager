\c partitions;

CREATE TABLE by_date (
    id              BIGSERIAL,
    temperature     INT,
    created_at      DATE NOT NULL
) PARTITION BY RANGE (created_at);

CREATE TABLE by_date_2000 PARTITION OF by_date FOR VALUES FROM ('2000-01-01') TO ('2001-01-01');
CREATE TABLE by_date_2001 PARTITION OF by_date FOR VALUES FROM ('2001-01-01') TO ('2002-01-01');
CREATE TABLE by_date_2002 PARTITION OF by_date FOR VALUES FROM ('2002-01-01') TO ('2003-01-01');

INSERT INTO by_date values (1, floor(RANDOM()*100), '2000-12-31');
INSERT INTO by_date values (2, floor(RANDOM()*100), '2001-12-31');
INSERT INTO by_date values (3, floor(RANDOM()*100), '2002-12-31');
