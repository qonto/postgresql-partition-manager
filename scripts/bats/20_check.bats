load 'test/libs/dependencies'
load 'test/libs/partitions'
load 'test/libs/seeds'
load 'test/libs/sql'

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert

  reset_database
}

@test "Test exit code on PostgreSQL connection error" {
  run postgresql-partition-manager run check -c configuration/valid.yaml --connection-url an-invalid-connection-url

  assert_failure
  assert_equal "$status" 3
  assert_output --partial "Could not connect to database"
}

@test "Test check return a success on valid configuration" {
  local TABLE=$(generate_table_name)
  local INTERVAL=daily
  local RETENTION=2
  local PREPROVISIONED=2

  # Create partioned table 2 retention days
  create_daily_partioned_table ${TABLE} ${RETENTION} ${PREPROVISIONED}

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

  run postgresql-partition-manager run check -c ${CONFIGURATION_FILE}

  assert_success
  assert_output --partial "All partitions are correctly configured"
}

@test "Test check return an error when retention partitions are missing" {
  local TABLE=$(generate_table_name)
  local INTERVAL=daily
  local INITIAL_RETENTION=1
  local NEW_RETENTION=2
  local PREPROVISIONED=1

  create_daily_partioned_table ${TABLE} ${TABLE} ${INITIAL_RETENTION} ${PREPROVISIONED}

  # Generate configuration with only 1 retention
  local CONFIGURATION=$(cat << EOF
partitions:
  unittest:
    schema: public
    table: ${TABLE}
    interval: daily
    partitionKey: created_at
    cleanupPolicy: drop
    retention: ${NEW_RETENTION} # This should generate an error
    preProvisioned: ${PREPROVISIONED}
EOF
)
  local CONFIGURATION_FILE=$(generate_configuration_file "${CONFIGURATION}")

  run postgresql-partition-manager run check -c ${CONFIGURATION_FILE}

  assert_failure
  assert_output --partial "Found missing tables"
}

@test "Test check return an error when preProvisioned partitions are missing" {
  local TABLE=$(generate_table_name)
  local INTERVAL=daily
  local RETENTION=2
  local INITIAL_PREPROVISIONED=2
  local NEW_PREPROVISIONED=3

  create_daily_partioned_table ${TABLE} ${RETENTION} ${INITIAL_PREPROVISIONED}

  # Increase preProvisioned partitions
  local CONFIGURATION=$(cat << EOF
partitions:
  unittest:
    schema: public
    table: ${TABLE}
    interval: daily
    partitionKey: created_at
    cleanupPolicy: drop
    retention: ${RETENTION}
    preProvisioned: ${NEW_PREPROVISIONED} # This will generate an error
EOF
)
  local CONFIGURATION_FILE=$(generate_configuration_file "${CONFIGURATION}")

  run postgresql-partition-manager run check -c ${CONFIGURATION_FILE}

  assert_failure
  assert_output --partial "Found missing tables"
}
