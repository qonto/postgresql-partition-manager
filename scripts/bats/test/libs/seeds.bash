init_database() {
  QUERY="CREATE DATABASE unittest;"
  execute_sql "${QUERY}" postgres
}

drop_database() {
  QUERY="set lock_timeout to '5s'; DROP DATABASE IF EXISTS unittest;"
  execute_sql_commands "${QUERY}" postgres
}

reset_database() {
  drop_database
  init_database
}

create_table_from_template() {
  local TABLE=$1

  read -r -d '' QUERY <<EOQ ||
  CREATE TABLE ${TABLE} (
    id              BIGSERIAL,
    temperature     INT,
    created_at      DATE NOT NULL
  ) PARTITION BY RANGE (created_at);
EOQ
  execute_sql "${QUERY}"
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

generate_configuration_file() {
  local PARTITION_CONFIGURATION=$1
  local CONFIGURATION_TEMPLATE_FILE=configuration/template.yaml

  local TEMPORARY_FILE=$(mktemp).yaml
  echo "${PARTITION_CONFIGURATION}" > "${TEMPORARY_FILE}"

  local FILENAME=$(mktemp).yaml
  yq '. as $item ireduce ({}; . * $item )' "${CONFIGURATION_TEMPLATE_FILE}" "${TEMPORARY_FILE}" > "${FILENAME}"

  echo $FILENAME
}
