create_partitioned_table() {
  create_table_from_template "$1"
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
  echo "tbl_"$(random_suffix)
}

# Generic method to create child partitions with specified ranges
# Arguments: parent table and an array of partition definitions
# The array elements are:
# [0] partition name
# [1]   lower bound
# [2]   upper bound
# [3] name of next partition
# [4]   lower bound for next partition
# ...and so on (3 elements per partition)
create_partitions() {
    local tbl="$1"
    shift
    local parts=("$@")

    local len=${#parts[*]}
    if ! (( len % 3 == 0 )); then
	echo >&2 "The list must have 3 elements per partition (found length=$len)"
	return 1
    fi
    i=0
    local sql_block="BEGIN;"
    while (( i < len ))
    do
	# Partition names and ranges must be SQL-quoted by the caller if needed
	sql_block="$sql_block
	           CREATE TABLE ${parts[i]} PARTITION OF ${tbl} FOR VALUES FROM ('${parts[((i+1))]}') TO ('${parts[((i+2))]}') ;"
	(( i+=3 ))
    done
    sql_block="$sql_block
		COMMIT;";
    execute_sql_commands "$sql_block"
}
