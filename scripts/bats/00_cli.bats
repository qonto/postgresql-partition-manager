setup() {
    bats_load_library bats-support
    bats_load_library bats-assert
}

@test "Test returns its version" {
  run postgresql-partition-manager --version

  assert_success
  assert_output --partial "postgresql-partition-manager version development"
}

@test "Test help message" {
  run postgresql-partition-manager --help

  assert_success
  assert_output --partial "Simplified PostgreSQL partioning management"
}

@test "Test exit code on PostgreSQL connection error" {
  run postgresql-partition-manager run check -c configuration/valid.yaml --connection-url an-invalid-connection-url

  assert_failure
  assert_equal "$status" 3
  assert_output --partial "Could not connect to database"
}
