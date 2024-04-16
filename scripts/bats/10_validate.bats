setup() {
    bats_load_library bats-support
    bats_load_library bats-assert
}

# Test for program's behavior when the configuration file is empty
@test "Program should exit when passed an empty configuration file" {
  run postgresql-partition-manager validate -c /dev/null

  assert_failure
  assert_equal "$status" 1

  # Verify that mandatory fields are reported as errors
  assert_line "ERROR: The 'Config.ConnectionURL' field is required and cannot be empty."
  assert_line "ERROR: The 'Config.Partitions' field is required and cannot be empty."
}

@test "Ensure validate command executes successfully with a valid configuration file" {
  run postgresql-partition-manager validate -c configuration/valid.yaml

  assert_success
  assert_output --partial "Configuration is valid"
}
