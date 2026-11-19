load 'test/libs/startup'
load 'test/libs/dependencies'
load 'test/libs/partitions'
load 'test/libs/seeds'
load 'test/libs/sql'

setup_file() {
  init_database
}

teardown_file() {
  drop_database
}

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  ppm_setup
}

@test "Test that obsolete partitions are removed by the cleanup" {
  local TABLE=$(generate_table_name)
  local INTERVAL=daily
  local START_RETENTION=3
  local START_PREPROVISIONED=3

  # Create partitioned table
  create_daily_partitioned_table ${TABLE} ${START_RETENTION} ${START_PREPROVISIONED}

  for ((i=1; i<= START_RETENTION; i++));do
    assert_table_exists public $(generate_daily_partition_name ${TABLE} -${i})
  done

  for ((i=1; i<= START_PREPROVISIONED; i++));do
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

  run "$PPM_PROG" run cleanup -c ${CONFIGURATION_FILE}

  assert_success
  assert_output --partial "All partitions are cleaned"
  assert_table_exists public $(generate_daily_partition_name ${TABLE} 1) # Should exist
  assert_table_exists public $(generate_daily_partition_name ${TABLE} 0) # current partition must still exists
  assert_table_exists public $(generate_daily_partition_name ${TABLE} -1) # Should exist

  for ((i=NEW_RETENTION +1; i<= START_RETENTION; i++));do
    assert_table_not_exists public $(generate_daily_partition_name ${TABLE} -${i})
  done

  for ((i=NEW_RETENTION +1; i<= START_PREPROVISIONED; i++));do
    assert_table_not_exists public $(generate_daily_partition_name ${TABLE} ${i})
  done

  rm "$CONFIGURATION_FILE"
}

@test "Test that gaps in the partition set prevent any partition removal" {
  local TABLE="test_uuid_gap"
  local INTERVAL=monthly
  local RETENTION=1
  local PREPROVISIONED=2

  create_table_uuid_range ${TABLE}

  declare -a PARTS=(
      ${TABLE}_2024_12 01937f84-5800-7000-8000-000000000000 01941f29-7c00-7000-8000-000000000000
      # next partition is missing
      # ${TABLE}_2025_01 01941f29-7c00-7000-8000-000000000000 0194bece-a000-7000-8000-000000000000
      ${TABLE}_2025_02 0194bece-a000-7000-8000-000000000000 01954f00-b000-7000-8000-000000000000
      ${TABLE}_2025_03 01954f00-b000-7000-8000-000000000000 0195eea5-d400-7000-8000-000000000000
      ${TABLE}_2025_04 0195eea5-d400-7000-8000-000000000000 01968924-9c00-7000-8000-000000000000
  )

  create_partitions "$TABLE" "${PARTS[@]}"

  local CONFIGURATION=$(cat << EOF
partitions:
  unittest:
    schema: public
    table: ${TABLE}
    interval: ${INTERVAL}
    partitionKey: id
    cleanupPolicy: drop
    retention: ${RETENTION}
    preProvisioned: ${PREPROVISIONED}
EOF
)
  local CONFIGURATION_FILE=$(generate_configuration_file "${CONFIGURATION}")

  # When run on 2025-03-15 with a retention of 1 month, the partition for 2024-12
  # is old enough to be dropped. But since 2025-01 is missing, it is an error that
  # should prevent the drop.
  PPM_WORK_DATE="2025-03-15" run "$PPM_PROG" run cleanup -c ${CONFIGURATION_FILE}

  assert_failure
  assert_output --partial 'level=ERROR msg="Partition Gap"'

  # Check that all the partitions are still there
  local i=0
  local expected=""
  while (( i < ${#PARTS[*]} ))
  do
    # bats does not expect any trailing newline, so append it to each previous line
    # except the first
    if (( i > 0 )); then expected+=$'\n'; fi
    expected+="public|${PARTS[i]}|${PARTS[i+1]}|${PARTS[i+2]}"
    (( i+=3 ))
  done
  run list_existing_partitions "public" "${TABLE}"

  assert_output "$expected"

  rm "$CONFIGURATION_FILE"
}

@test "Test cleanup with initialRetention=1 and retention=3" {
  local TABLE=$(generate_table_name)
  local INTERVAL=daily
  local RETENTION=3
  local INITIAL_RETENTION=1
  local PREPROVISIONED=1

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
    initialRetention: ${INITIAL_RETENTION}
    preProvisioned: ${PREPROVISIONED}
EOF
)
  local CONFIGURATION_FILE=$(generate_configuration_file "${CONFIGURATION}")

  # Simulate time going by: provision then cleanup each day.
  # With retention=3 the partitions whose upper bound is <= (work date - 3 days)
  # are dropped. initialRetention only concerns provisioning and must not
  # affect which partitions are dropped.
  declare -A EXPECTED

  EXPECTED[09]=$(cat << EOF
public|${TABLE}_2026_03_08|2026-03-08|2026-03-09
public|${TABLE}_2026_03_09|2026-03-09|2026-03-10
public|${TABLE}_2026_03_10|2026-03-10|2026-03-11
EOF
)

  EXPECTED[10]=$(cat << EOF
public|${TABLE}_2026_03_08|2026-03-08|2026-03-09
public|${TABLE}_2026_03_09|2026-03-09|2026-03-10
public|${TABLE}_2026_03_10|2026-03-10|2026-03-11
public|${TABLE}_2026_03_11|2026-03-11|2026-03-12
EOF
)

  EXPECTED[11]=$(cat << EOF
public|${TABLE}_2026_03_08|2026-03-08|2026-03-09
public|${TABLE}_2026_03_09|2026-03-09|2026-03-10
public|${TABLE}_2026_03_10|2026-03-10|2026-03-11
public|${TABLE}_2026_03_11|2026-03-11|2026-03-12
public|${TABLE}_2026_03_12|2026-03-12|2026-03-13
EOF
)

  EXPECTED[12]=$(cat << EOF
public|${TABLE}_2026_03_09|2026-03-09|2026-03-10
public|${TABLE}_2026_03_10|2026-03-10|2026-03-11
public|${TABLE}_2026_03_11|2026-03-11|2026-03-12
public|${TABLE}_2026_03_12|2026-03-12|2026-03-13
public|${TABLE}_2026_03_13|2026-03-13|2026-03-14
EOF
)

  EXPECTED[13]=$(cat << EOF
public|${TABLE}_2026_03_10|2026-03-10|2026-03-11
public|${TABLE}_2026_03_11|2026-03-11|2026-03-12
public|${TABLE}_2026_03_12|2026-03-12|2026-03-13
public|${TABLE}_2026_03_13|2026-03-13|2026-03-14
public|${TABLE}_2026_03_14|2026-03-14|2026-03-15
EOF
)

  local d
  for d in 09 10 11 12 13; do
    PPM_WORK_DATE="2026-03-${d}" run "$PPM_PROG" run provisioning -c ${CONFIGURATION_FILE}
    assert_success
    assert_output --partial "All partitions are correctly provisioned"

    PPM_WORK_DATE="2026-03-${d}" run "$PPM_PROG" run cleanup -c ${CONFIGURATION_FILE}
    assert_success
    assert_output --partial "All partitions are cleaned"

    run list_existing_partitions "public" "${TABLE}"
    assert_output "${EXPECTED[$d]}"
  done

  rm "$CONFIGURATION_FILE"
}
