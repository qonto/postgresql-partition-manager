\c partitions;

CREATE OR REPLACE FUNCTION min_uuid_v7(min_timestamp timestamp) RETURNS uuid
LANGUAGE sql immutable strict
AS $function$
  SELECT encode(
      overlay('\x00000000000070000000000000000000'::bytea
          placing substring(int8send(floor(extract(epoch from min_timestamp) * 1000)::bigint) from 3)
          from 1 for 6)
      , 'hex')::uuid;
$function$;

CREATE TABLE by_uuidv7 (
    id              UUID,
    temperature     INT
) PARTITION BY RANGE (id);

CREATE TABLE by_uuidv7_2000 PARTITION OF by_uuidv7 FOR VALUES FROM (min_uuid_v7('2000-01-01')) TO (min_uuid_v7('2001-01-01'));
CREATE TABLE by_uuidv7_2001 PARTITION OF by_uuidv7 FOR VALUES FROM (min_uuid_v7('2001-01-01')) TO (min_uuid_v7('2002-01-01'));
CREATE TABLE by_uuidv7_2002 PARTITION OF by_uuidv7 FOR VALUES FROM (min_uuid_v7('2002-01-01')) TO (min_uuid_v7('2003-01-01'));

INSERT INTO by_uuidv7 values (min_uuid_v7('2000-01-01'), floor(RANDOM()*100));
INSERT INTO by_uuidv7 values (min_uuid_v7('2001-01-01'), floor(RANDOM()*100));
INSERT INTO by_uuidv7 values (min_uuid_v7('2002-01-01'), floor(RANDOM()*100));
