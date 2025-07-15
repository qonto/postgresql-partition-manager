execute_sql() {
  local SQL_COMMAND=$1
  local DATABASE=$2

  if [ -z "$DATABASE" ]; then
    psql --echo-all --echo-errors -c "${SQL_COMMAND}"
  else
    psql --echo-all --echo-errors --dbname="${DATABASE}" -c "${SQL_COMMAND}"
  fi
}

execute_sql_file() {
  local SQL_FILE=$1
  local DATABASE=$2

  if [ -z "$DATABASE" ]; then
    psql --echo-all --echo-errors -f "${SQL_FILE}"
  else
    psql --echo-all --echo-errors --dbname="${DATABASE}" -f "${SQL_FILE}"
  fi
}

# Execute multiple semicolon-separated commands
execute_sql_commands() {
  # $1: sql commands
  # $2: dbname (if unset, psql defaults to $PGDATABASE and then $USER)

  local command="psql --tuples-only --no-align --quiet"

  if [ ! -z "$2" ]; then
    command="$command --dbname=$2"
  fi

  $command <<EOSQL
$1
EOSQL
}

list_existing_partitions() {
  # mandatory arguments
  local PARENT_SCHEMA=$1
  local PARENT_TABLE=$2

  psql --tuples-only --no-align --quiet --dbname="$PPM_DATABASE" -v parent_schema=${PARENT_SCHEMA} -v parent_table=${PARENT_TABLE} <<'EOSQL'
WITH parts as (
	SELECT
	   relnamespace::regnamespace as schema,
	   c.oid::pg_catalog.regclass AS part_name,
	   regexp_match(pg_get_expr(c.relpartbound, c.oid),
				  'FOR VALUES FROM \(''(.*)''\) TO \(''(.*)''\)') AS bounds
	 FROM
	   pg_catalog.pg_class c JOIN pg_catalog.pg_inherits i ON (c.oid = i.inhrelid)
	 WHERE i.inhparent = (select oid from pg_class where relnamespace=:'parent_schema'::regnamespace and relname=:'parent_table' and relkind='p')
	   AND c.relkind='r'
)
SELECT
	schema,
	part_name as name,
	bounds[1]::text AS lowerBound,
	bounds[2]::text AS upperBound
FROM parts
ORDER BY bounds[1]::text COLLATE "C";

EOSQL
}

assert_table_exists() {
  local SCHEMA="$1"
  local TABLE="$2"

  local QUERY="SELECT EXISTS (
    SELECT
    FROM information_schema.tables
    WHERE table_schema = '${SCHEMA}'
    AND table_name = '${TABLE}'
  );"

  run psql --dbname="$PPM_DATABASE" --tuples-only --no-align -c "${QUERY}"

  assert_success
  assert_output t
}

assert_table_not_exists() {
  local SCHEMA="$1"
  local TABLE="$2"

  local QUERY="SELECT EXISTS (
    SELECT
    FROM information_schema.tables
    WHERE table_schema = '${SCHEMA}'
    AND table_name = '${TABLE}'
  );"

  run psql --dbname="$PPM_DATABASE" --tuples-only --no-align -c "${QUERY}"

  assert_success
  assert_output f
}
