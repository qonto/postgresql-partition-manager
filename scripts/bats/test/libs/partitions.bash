create_partitioned_table() {
  local TABLE=$1
  local INTERVAL=$2
  local RETENTION=$3
  local PREPROVISIONED=$4

  create_table_from_template ${TABLE}
}

create_daily_partitioned_table() {
  local TABLE=$1
  local RETENTION=$2
  local PREPROVISIONED=$3

  create_table_from_template ${TABLE} "daily" ${RETENTION} ${PREPROVISIONED}
  generate_daily_partitions ${TABLE} ${RETENTION} ${PREPROVISIONED}
}

generate_daily_partitions() {
  local PARENT_TABLE=$1
  local RETENTION=$2
  local PREPROVISIONED=$3

  # Generate retention partitions
  for ((i=1; i<=RETENTION; i++))
  do
    generate_daily_partition ${PARENT_TABLE} -$i
  done

  # Generate current partition
  generate_daily_partition ${PARENT_TABLE} 0

  # Generate preProvisioned partitions
  for ((i=1; i<=PREPROVISIONED; i++))
  do
    generate_daily_partition ${PARENT_TABLE} $i
  done
}

generate_daily_partition() {
  local PARENT_TABLE=$1
  local TIMEDELTA=$2

  local TABLE_NAME=$(generate_daily_partition_name "${PARENT_TABLE}" "${TIMEDELTA}")
  local LOWER_BOUND=$(date -d "@$(( $(date +%s) + 86400 * $TIMEDELTA))" +"%Y-%m-%d")
  local UPPER_BOUND=$(date -d "@$(( $(date +%s) + 86400 * $TIMEDELTA + 86400))" +"%Y-%m-%d")

  local QUERY="CREATE TABLE ${TABLE_NAME} PARTITION OF ${PARENT_TABLE} FOR VALUES FROM ('${LOWER_BOUND}') TO ('${UPPER_BOUND}');"
  execute_sql "${QUERY}"
}

generate_daily_partition_name() {
  local PARENT_TABLE=$1
  local TIMEDELTA=$2

  echo $(date -d "@$(( $(date +%s) + 86400 * $TIMEDELTA))" +"${PARENT_TABLE}_%Y_%m_%d")
}

generate_table_name() {
  cat /dev/urandom | head -n 1 | base64 | tr -dc '[:alnum:]' | tr '[:upper:]' '[:lower:]' | cut -c -13 | sed -e 's/^[0-9]/a/g'
}
