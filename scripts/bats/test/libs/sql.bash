execute_sql() {
  local SQL_COMMAND=$1
  local DATABASE=$2

  if [ -z ${DATABASE} ]; then
    psql --echo-all -c "${SQL_COMMAND}"
  else
    psql --echo-all --dbname="${DATABASE}" -c "${SQL_COMMAND}"
  fi
}

execute_sql_file() {
  local SQL_FILE=$1
  local DATABASE=$2

  if [ -z ${DATABASE} ]; then
    psql --echo-all -f "${SQL_FILE}"
  else
    psql --echo-all --dbname="${DATABASE}" -f "${SQL_FILE}"
  fi
}

assert_table_exists() {
  local SCHEMA=$1
  local TABLE=$2

  local QUERY="SELECT EXISTS (
    SELECT
    FROM information_schema.tables
    WHERE table_schema = '${SCHEMA}'
    AND table_name = '${TABLE}'
  );"

  run psql --tuples-only --no-align -c "${QUERY}"

  assert_success
  assert_output t
}

assert_table_not_exists() {
  local SCHEMA=$1
  local TABLE=$2

  local QUERY="SELECT EXISTS (
    SELECT
    FROM information_schema.tables
    WHERE table_schema = '${SCHEMA}'
    AND table_name = '${TABLE}'
  );"

  run psql --tuples-only --no-align -c "${QUERY}"

  assert_success
  assert_output f
}
