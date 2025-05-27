load 'test/libs/dependencies'
load 'test/libs/partitions'
load 'test/libs/seeds'
load 'test/libs/sql'

setup_file() {
  reset_database
}

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
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

@test "Test that gaps in the partition set prevent any partition removal" {
  local TABLE="test_uuid_gap"
  local INTERVAL=monthly
  local RETENTION=1
  local PREPROVISIONED=2

  create_table_uuid_range ${TABLE}

  declare -a PARTS=(
      ${TABLE}_2024_12 01937f84-5800-7000-0000-000000000000 01941f29-7c00-7000-0000-000000000000
      # next partition is missing
      # ${TABLE}_2025_01 01941f29-7c00-7000-0000-000000000000 0194bece-a000-7000-0000-000000000000
      ${TABLE}_2025_02 0194bece-a000-7000-0000-000000000000 01954f00-b000-7000-0000-000000000000
      ${TABLE}_2025_03 01954f00-b000-7000-0000-000000000000 0195eea5-d400-7000-0000-000000000000
      ${TABLE}_2025_04 0195eea5-d400-7000-0000-000000000000 01968924-9c00-7000-0000-000000000000
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
  PPM_WORK_DATE="2025-03-15" run postgresql-partition-manager run cleanup -c ${CONFIGURATION_FILE}

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
  run list_existing_partitions "unittest" "public" "${TABLE}"

  assert_output "$expected"

  rm "$CONFIGURATION_FILE"
}
