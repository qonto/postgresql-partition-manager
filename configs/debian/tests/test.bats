#!/usr/bin/env bats

load '/usr/lib/bats/bats-support/load'
load '/usr/lib/bats/bats-assert/load'
load '/usr/lib/bats/bats-file/load'

PACKAGE=/mnt/postgresql-partition-manager.deb

setup() {
  run bash -c "DEBIAN_FRONTEND=noninteractive sudo dpkg -i ${PACKAGE}"
  assert_success
}

remove_package() {
  run bash -c 'sudo apt-get remove -y postgresql-partition-manager'
  assert_success
}

purge_package() {
  run bash -c 'sudo apt-get purge -y postgresql-partition-manager'
  assert_success
}

@test "Test installation" {
  assert_file_exist /usr/share/postgresql-partition-manager/postgresql-partition-manager.yaml.sample

  run bash -c 'postgresql-partition-manager --version'
  assert_success
  assert_output --regexp '^postgresql-partition-manager version'

  run bash -c "dpkg --info ${PACKAGE}"
  assert_output --regexp 'Package: postgresql-partition-manager'
  assert_output --regexp 'Streamline the management of PostgreSQL partitions'
  assert_output --regexp 'Maintainer: SRE Team'
  assert_output --regexp 'Homepage: https://github.com/qonto/postgresql-partition-manager'
}

@test 'Check removed package' {
  remove_package

  assert_file_not_exist /usr/bin/postgresql-partition-manager
}

@test 'Check purged package' {
  purge_package

  assert_file_not_exist /usr/bin/postgresql-partition-manager
  assert_file_not_exist /usr/share/postgresql-partition-manager/postgresql-partition-manager.yaml.sample
}
