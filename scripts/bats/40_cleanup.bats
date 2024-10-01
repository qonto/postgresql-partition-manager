load 'test/libs/dependencies'
load 'test/libs/partitions'
load 'test/libs/seeds'
load 'test/libs/sql'

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert

  reset_database
}

@test "Test that useless partitions are removed by the cleanup" {
  local TABLE=$(generate_table_name)
  local INTERVAL=daily
  local INITIAL_RETENTION=3
  local INITIAL_PREPROVISIONED=3

  # Create partitioned table
  create_daily_partitioned_table ${TABLE} ${INITIAL_RETENTION} ${INITIAL_PREPROVISIONED}

  for ((i=1; i<= INITIAL_RETENTION; i++));do
    assert_table_exists public $(generate_daily_partition_name ${TABLE} -${i})
  done

  for ((i=1; i<= INITIAL_PREPROVISIONED; i++));do
    assert_table_exists public $(generate_daily_partition_name ${TABLE} ${i})
  done

  # Set lower retention and preProvisioned
  local NEW_RETENTION=1
  local NEW_PREPROVISIONED=1
  local CONFIGURATION=$(cat << EOF
partitions:
  unittest:
    schema: public
    table: ${TABLE}
    interval: ${INTERVAL}
    partitionKey: created_at
    cleanupPolicy: drop
    retention: ${NEW_RETENTION}
    preProvisioned: ${NEW_PREPROVISIONED}
EOF
)
  local CONFIGURATION_FILE=$(generate_configuration_file "${CONFIGURATION}")

  run postgresql-partition-manager run cleanup -c ${CONFIGURATION_FILE}

  assert_success
  assert_output --partial "All partitions are cleaned"
  assert_table_exists public $(generate_daily_partition_name ${TABLE} 1) # Should exist
  assert_table_exists public $(generate_daily_partition_name ${TABLE} 0) # current partition must still exists
  assert_table_exists public $(generate_daily_partition_name ${TABLE} -1) # Should exist

  for ((i=NEW_RETENTION +1; i<= INITIAL_RETENTION; i++));do
    assert_table_not_exists public $(generate_daily_partition_name ${TABLE} -${i})
  done

  for ((i=NEW_RETENTION +1; i<= INITIAL_PREPROVISIONED; i++));do
    assert_table_not_exists public $(generate_daily_partition_name ${TABLE} ${i})
  done
}
