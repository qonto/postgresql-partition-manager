install_yq() {
    apk add yq
}

install_psql() {
    apk add postgresql-client
}

install_dependencies() {
    install_yq
    install_psql
}
