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

.PHONY: helm-test
helm-test:
	helm unittest configs/helm

.PHONY: kubeconform-test
kubeconform-test:
	./scripts/kubeconform-test.sh configs/helm

.PHONY: goreleaser-check
goreleaser-check:
	goreleaser check

debian-test:
	GORELEASER_CURRENT_TAG=0.0.0 goreleaser release --clean --skip-publish --skip-docker --snapshot
	docker build configs/debian/tests -t test
	docker run -v ./dist/postgresql-partition-manager_0.0.1~next_$(ARCHITECTURE).deb:/mnt/postgresql-partition-manager.deb test

debian-test-ci:
	docker build configs/debian/tests -t test
	docker run -v ./dist/postgresql-partition-manager_0.0.1~next_amd64.deb:/mnt/postgresql-partition-manager.deb test

.PHONY: test
test:
	go test -race -v ./... -coverprofile=coverage.txt -covermode atomic
	go run github.com/boumenot/gocover-cobertura@latest < coverage.txt > coverage.xml
	go tool cover -html coverage.txt -o cover.html

.PHONY: lint
lint:
	golangci-lint run --verbose --timeout 2m

.PHONY: all-tests
all-tests: test helm-test kubeconform-test goreleaser-check
