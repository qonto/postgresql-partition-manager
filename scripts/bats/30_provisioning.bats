load 'test/libs/dependencies'
load 'test/libs/partitions'
load 'test/libs/seeds'
load 'test/libs/sql'

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert

  reset_database
}

@test "Test that provisioning succeed on up-to-date partitioning" {
  local TABLE=$(generate_table_name)
  local INTERVAL=daily
  local RETENTION=1
  local PREPROVISIONED=1

  # Create partioned table
  create_partioned_table ${TABLE} ${INTERVAL} ${RETENTION} ${PREPROVISIONED}

  local CONFIGURATION=$(cat << EOF
partitions:
  unittest:
    schema: public
    table: ${TABLE}
    interval: ${INTERVAL}
    partitionKey: created_at
    cleanupPolicy: drop
    retention: ${RETENTION}
    preProvisioned: ${PREPROVISIONED}
EOF
)
  local CONFIGURATION_FILE=$(generate_configuration_file "${CONFIGURATION}")

  run postgresql-partition-manager run provisioning -c ${CONFIGURATION_FILE}

  assert_success
  assert_output --partial "All partitions are correctly provisioned"
  assert_table_exists public $(generate_daily_partition_name ${TABLE} -1) # retention partition
  assert_table_exists public $(generate_daily_partition_name ${TABLE} 0) # current partition
  assert_table_exists public $(generate_daily_partition_name ${TABLE} 1) # preProvisioned partition
}

@test "Test that preProvisioned and retention partitions can be increased" {
  local TABLE=$(generate_table_name)
  local INTERVAL=daily
  local RETENTION=1
  local PREPROVISIONED=1

  # Create partioned table
  create_partioned_table ${TABLE} ${INTERVAL} ${RETENTION} ${PREPROVISIONED}

  local CONFIGURATION=$(cat << EOF
partitions:
  unittest:
    schema: public
    table: ${TABLE}
    interval: ${INTERVAL}
    partitionKey: created_at
    cleanupPolicy: drop
    retention: ${RETENTION}
    preProvisioned: ${PREPROVISIONED}
EOF
)
  local CONFIGURATION_FILE=$(generate_configuration_file "${CONFIGURATION}")

  run postgresql-partition-manager run provisioning -c ${CONFIGURATION_FILE}


  assert_success
  assert_output --partial "All partitions are correctly provisioned"
  assert_table_exists public $(generate_daily_partition_name ${TABLE} -1) # retention partition
  assert_table_exists public $(generate_daily_partition_name ${TABLE} 0) # current partition
  assert_table_exists public $(generate_daily_partition_name ${TABLE} 1) # preProvisioned partition

  # Increase retention and preProvisionned partitions
  local NEW_RETENTION=2
  local NEW_PREPROVISIONED=3
  yq eval ".partitions.unittest.retention = ${NEW_RETENTION}" -i ${CONFIGURATION_FILE}
  yq eval ".partitions.unittest.preProvisioned = ${NEW_PREPROVISIONED}" -i ${CONFIGURATION_FILE}

  run postgresql-partition-manager run provisioning -c ${CONFIGURATION_FILE}

  assert_success
  assert_output --partial "All partitions are correctly provisioned"
  assert_table_exists public $(generate_daily_partition_name ${TABLE} -${NEW_RETENTION}) # New retention partition
  assert_table_exists public $(generate_daily_partition_name ${TABLE} ${NEW_PREPROVISIONED}) # New preProvisioned partition
}
