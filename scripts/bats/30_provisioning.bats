load 'test/libs/dependencies'
load 'test/libs/partitions'
load 'test/libs/seeds'
load 'test/libs/sql'
load 'test/libs/time'

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

  # Create partitioned table
  create_partitioned_table ${TABLE}

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

  # Create partitioned table
  create_partitioned_table ${TABLE}

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

  # Increase retention and preProvisioned partitions
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

@test "Test monthly partitions" {
  local TABLE=$(generate_table_name)
  local INTERVAL=monthly
  local RETENTION=1
  local PREPROVISIONED=1
  local EXPECTED_LAST_TABLE="${TABLE}_$(get_current_date_adjusted_by_month -1)"
  local EXPECTED_CURRENT_TABLE="${TABLE}_$(get_current_date_adjusted_by_month 0)"
  local EXPECTED_NEXT_TABLE="${TABLE}_$(get_current_date_adjusted_by_month +1)"

  # Create partitioned table
  create_partitioned_table ${TABLE}

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

  cat ${CONFIGURATION_FILE}

  run postgresql-partition-manager run provisioning -c ${CONFIGURATION_FILE}

  assert_success
  assert_output --partial "All partitions are correctly provisioned"
  assert_table_exists public ${EXPECTED_LAST_TABLE}
  assert_table_exists public ${EXPECTED_CURRENT_TABLE}
  assert_table_exists public ${EXPECTED_NEXT_TABLE}
}

@test "Test quarterly partitions" {
  local TABLE=$(generate_table_name)
  local INTERVAL=quarterly
  local RETENTION=1
  local PREPROVISIONED=1
  local EXPECTED_LAST_TABLE="${TABLE}_$(get_current_date_adjusted_by_quarter -1)"
  local EXPECTED_CURRENT_TABLE="${TABLE}_$(get_current_date_adjusted_by_quarter 0)"
  local EXPECTED_NEXT_TABLE="${TABLE}_$(get_current_date_adjusted_by_quarter +1)"

  # Create partitioned table
  create_partitioned_table ${TABLE}

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
  assert_table_exists public ${EXPECTED_LAST_TABLE}
  assert_table_exists public ${EXPECTED_CURRENT_TABLE}
  assert_table_exists public ${EXPECTED_NEXT_TABLE}
}

@test "Test yearly partitions" {
  local TABLE=$(generate_table_name)
  local INTERVAL=yearly
  local RETENTION=1
  local PREPROVISIONED=1
  local EXPECTED_LAST_TABLE="${TABLE}_$(get_current_date_adjusted_by_year -1)"
  local EXPECTED_CURRENT_TABLE="${TABLE}_$(get_current_date_adjusted_by_year 0)"
  local EXPECTED_NEXT_TABLE="${TABLE}_$(get_current_date_adjusted_by_year +1)"

  # Create partitioned table
  create_partitioned_table ${TABLE}

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
  assert_table_exists public ${EXPECTED_LAST_TABLE}
  assert_table_exists public ${EXPECTED_CURRENT_TABLE}
  assert_table_exists public ${EXPECTED_NEXT_TABLE}
}

@test "Test interval change" {
  # change monthly to weekly
  local TABLE="test_interv"
  local INTERVAL=monthly
  local RETENTION=1
  local PREPROVISIONED=1

  # Create partitioned table
  create_partitioned_table ${TABLE}

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

  PPM_WORK_DATE="2025-01-20" run postgresql-partition-manager run provisioning -c ${CONFIGURATION_FILE}
  assert_success
  assert_output --partial "All partitions are correctly provisioned"

  run list_existing_partitions "unittest" "public" ${TABLE}

  local expected_monthly=$(cat <<EOF
public|test_interv_2024_12|2024-12-01|2025-01-01
public|test_interv_2025_01|2025-01-01|2025-02-01
public|test_interv_2025_02|2025-02-01|2025-03-01
EOF
  )

  assert_output "$expected_monthly"

  # Switch to weekly on 1st Feb 2025 with 10 weeks ahead
  yq eval ".partitions.unittest.interval = \"weekly\"" -i ${CONFIGURATION_FILE}
  yq eval ".partitions.unittest.retention = 2" -i ${CONFIGURATION_FILE}
  yq eval ".partitions.unittest.preProvisioned = 10" -i ${CONFIGURATION_FILE}

  PPM_WORK_DATE="2025-02-01" run postgresql-partition-manager run provisioning -c ${CONFIGURATION_FILE}
  assert_success

  local expected_mix=$(cat <<EOF
public|test_interv_2024_12|2024-12-01|2025-01-01
public|test_interv_2025_01|2025-01-01|2025-02-01
public|test_interv_2025_02|2025-02-01|2025-03-01
public|test_interv_20250301_20250303|2025-03-01|2025-03-03
public|test_interv_2025_w10|2025-03-03|2025-03-10
public|test_interv_2025_w11|2025-03-10|2025-03-17
public|test_interv_2025_w12|2025-03-17|2025-03-24
public|test_interv_2025_w13|2025-03-24|2025-03-31
public|test_interv_2025_w14|2025-03-31|2025-04-07
public|test_interv_2025_w15|2025-04-07|2025-04-14
EOF
  )

  run list_existing_partitions "unittest" "public" ${TABLE}
  assert_output "$expected_mix"

}

@test "Test provisioning with multiple partition sets in the configuration" {
  local CONFIGURATION=$(cat << EOF
partitions:
  unittest1:
    schema: public
    table: table_unittest1
    interval: daily
    partitionKey: created_at
    cleanupPolicy: detach
    retention: 1
    preProvisioned: 1
  unittest2:
    schema: public
    table: table_unittest2
    interval: daily
    partitionKey: created_at
    cleanupPolicy: drop
    retention: 2
    preProvisioned: 2
EOF
)
  local CONFIGURATION_FILE=$(generate_configuration_file "${CONFIGURATION}")

  create_partitioned_table "table_unittest1"
  create_partitioned_table "table_unittest2"

  PPM_WORK_DATE="2025-02-01" run postgresql-partition-manager run provisioning -c ${CONFIGURATION_FILE}
  assert_success

  local expected1=$(cat <<'EOF'
public|table_unittest1_2025_01_31|2025-01-31|2025-02-01
public|table_unittest1_2025_02_01|2025-02-01|2025-02-02
public|table_unittest1_2025_02_02|2025-02-02|2025-02-03
EOF
  )
  run list_existing_partitions "unittest" "public" "table_unittest1"
  assert_output "$expected1"

  local expected2=$(cat <<'EOF'
public|table_unittest2_2025_01_30|2025-01-30|2025-01-31
public|table_unittest2_2025_01_31|2025-01-31|2025-02-01
public|table_unittest2_2025_02_01|2025-02-01|2025-02-02
public|table_unittest2_2025_02_02|2025-02-02|2025-02-03
public|table_unittest2_2025_02_03|2025-02-03|2025-02-04
EOF
  )

  run list_existing_partitions "unittest" "public" "table_unittest2"
  assert_output "$expected2"
}

@test "Test that provisioning continues after an error on a partition set" {
  # Have a non-existing parent table, plus a normal set of partitions
  local CONFIGURATION=$(cat << EOF
partitions:
  unittest1:
    schema: public
    table: DOES_NOT_EXIST
    interval: daily
    partitionKey: created_at
    cleanupPolicy: detach
    retention: 1
    preProvisioned: 1
  unittest2:
    schema: public
    table: table_unittest2
    interval: daily
    partitionKey: created_at
    cleanupPolicy: drop
    retention: 2
    preProvisioned: 2
EOF
)
  local CONFIGURATION_FILE=$(generate_configuration_file "${CONFIGURATION}")

  create_partitioned_table "table_unittest2"

  PPM_WORK_DATE="2025-02-01" run postgresql-partition-manager run provisioning -c ${CONFIGURATION_FILE}

  # The status must reflect the fact that one partition set failed
  [ "$status" -eq 4 ]  # PartitionsProvisioningFailedExitCode

  # Check the success of the second set of partitions
  local expected2=$(cat <<'EOF'
public|table_unittest2_2025_01_30|2025-01-30|2025-01-31
public|table_unittest2_2025_01_31|2025-01-31|2025-02-01
public|table_unittest2_2025_02_01|2025-02-01|2025-02-02
public|table_unittest2_2025_02_02|2025-02-02|2025-02-03
public|table_unittest2_2025_02_03|2025-02-03|2025-02-04
EOF
  )

  run list_existing_partitions "unittest" "public" "table_unittest2"
  assert_output "$expected2"
}
