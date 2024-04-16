SHELL=/bin/bash -o pipefail
BUILD_INFO_PACKAGE_PATH=github.com/qonto/postgresql-partition-manager/internal/infra/build
BUILD_DATE=$(shell date -u +'%Y-%m-%dT%H:%M:%SZ')
GIT_COMMIT_SHA=$(shell git rev-parse HEAD)
BINARY=postgresql-partition-manager
ARCHITECTURE=$(shell uname -m)

all: build

.PHONY: format
format:
	gofumpt -l -w .

.PHONY: build
build:
	CGO_ENABLED=0 go build -v -ldflags="-X '$(BUILD_INFO_PACKAGE_PATH).Version=development' -X '$(BUILD_INFO_PACKAGE_PATH).CommitSHA=$(GIT_COMMIT_SHA)' -X '$(BUILD_INFO_PACKAGE_PATH).Date=$(BUILD_DATE)'" -o $(BINARY)

.PHONY: run
run:
	./$(BINARY) $(args)

.PHONY: install
install: build
	GOBIN=/usr/local/bin/ go install -v -ldflags="-X '$(BUILD_INFO_PACKAGE_PATH).Version=development' -X '$(BUILD_INFO_PACKAGE_PATH).CommitSHA=$(GIT_COMMIT_SHA)' -X '$(BUILD_INFO_PACKAGE_PATH).Date=$(BUILD_DATE)'"

.PHONY: bats-test
bats-test:
	cd scripts/bats && bats *.bats

.PHONY: test
test:
	go test -race -v ./... -coverprofile=coverage.txt -covermode atomic
	go run github.com/boumenot/gocover-cobertura@latest < coverage.txt > coverage.xml
	go tool cover -html coverage.txt -o cover.html

.PHONY: lint
lint:
	golangci-lint run --verbose --timeout 2m

.PHONY: goreleaser-check
goreleaser-check:
	goreleaser check

.PHONY: all-tests
all-tests: test goreleaser-check
