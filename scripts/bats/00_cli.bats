load 'test/libs/startup'

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  ppm_setup
}

@test "Test returns its version" {
  run "$PPM_PROG" --version

  assert_success
  assert_output --partial "postgresql-partition-manager version development"
}

@test "Test help message" {
  run "$PPM_PROG" --help

  assert_success
  assert_output --partial "Simplified PostgreSQL partitioning management"
}
