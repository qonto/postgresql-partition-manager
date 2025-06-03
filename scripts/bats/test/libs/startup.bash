ppm_setup() {
    DIR="$( cd "$( dirname "$BATS_TEST_FILENAME" )" >/dev/null 2>&1 && pwd )"
    PPM_PROG="$DIR/../../test-postgresql-partition-manager"
}
