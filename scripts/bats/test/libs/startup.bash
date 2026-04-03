ppm_setup() {
    PPM_PROG="/test-postgresql-partition-manager"
    if [ ! -x "$PPM_PROG" ]; then
        # Fallback for local (non-Docker) runs
        DIR="$( cd "$( dirname "$BATS_TEST_FILENAME" )" >/dev/null 2>&1 && pwd )"
        PPM_PROG="$DIR/../../test-postgresql-partition-manager"
    fi
}
