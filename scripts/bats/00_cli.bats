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
