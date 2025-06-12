load 'test/libs/startup'

setup() {
    bats_load_library bats-support
    bats_load_library bats-assert
    ppm_setup
}

# Test for program's behavior when the configuration file is empty
@test "Program should exit when passed an empty configuration file" {
  run "$PPM_PROG" validate -c /dev/null

  assert_failure
  assert_equal "$status" 1

  # Verify that mandatory fields are reported as errors
  assert_line "ERROR: The 'Config.Partitions' field is required and cannot be empty."
}

@test "Ensure validate command executes successfully with a valid configuration file" {
  run "$PPM_PROG" validate -c configuration/valid.yaml

  assert_success
  assert_output --partial "Configuration is valid"
}
