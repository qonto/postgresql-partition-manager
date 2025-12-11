# Emit a random string suitable for unquoted database identifiers (lower case, ASCII)
# The result may start with a number.
random_suffix() {
  head -c 12 /dev/urandom | base64 | tr -dc '[:alnum:]' | tr '[:upper:]' '[:lower:]'
}

init_database() {
  local dbname="ppm_test_"$(random_suffix)
  QUERY="CREATE DATABASE \"$dbname\" ;"
  execute_sql "${QUERY}" postgres
  export PPM_DATABASE="$dbname"
  export PGDATABASE="$dbname"
}

drop_database() {
  QUERY="set lock_timeout to '5s'; DROP DATABASE IF EXISTS \"$PPM_DATABASE\" ;"
  execute_sql_commands "${QUERY}" postgres
  unset PPM_DATABASE
}

reset_database() {
  drop_database
  init_database
}

create_table_from_template() {
  local TABLE=$1

  read -r -d '' QUERY <<EOQ ||
  CREATE TABLE ${TABLE} (
    id              BIGSERIAL NOT NULL,
    temperature     INT,
    created_at      DATE NOT NULL
  ) PARTITION BY RANGE (created_at);
EOQ
  execute_sql "${QUERY}" "${PPM_DATABASE}"
}

create_table_uuid_range() {
  local TABLE="$1"

  read -r -d '' QUERY <<EOQ ||
  CREATE TABLE ${TABLE} (
    id              uuid,
    value	    INT,
    created_at      DATE NOT NULL
  ) PARTITION BY RANGE (id);
EOQ
  execute_sql "${QUERY}"
}

create_table_timestamptz_range() {
  local TABLE="$1"

  read -r -d '' QUERY <<EOQ ||
  CREATE TABLE ${TABLE} (
    id              BIGSERIAL,
    value	    INT,
    created_at      TIMESTAMPTZ NOT NULL
  ) PARTITION BY RANGE (created_at);
EOQ
  execute_sql "${QUERY}"
}

generate_configuration_file() {
  local PARTITION_CONFIGURATION=$1
  local CONFIGURATION_TEMPLATE_FILE=configuration/template.yaml

  local TEMPORARY_FILE=$(mktemp).yaml
  echo "${PARTITION_CONFIGURATION}" > "${TEMPORARY_FILE}"

  local FILENAME=$(mktemp).yaml
  yq '. as $item ireduce ({}; . * $item )' "${CONFIGURATION_TEMPLATE_FILE}" "${TEMPORARY_FILE}" > "${FILENAME}"
  rm "${TEMPORARY_FILE}"
  echo "$FILENAME"
}

# Return a common configuration
# Arguments: table interval partition-key retention preprovisioned
basic_configuration() {
cat << EOF_conf
partitions:
  unittest:
    schema: public
    table: $1
    interval: $2
    partitionKey: $3
    cleanupPolicy: drop
    retention: $4
    preProvisioned: $5
EOF_conf
}
