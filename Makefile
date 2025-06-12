SHELL=/bin/bash -o pipefail
BUILD_INFO_PACKAGE_PATH=github.com/qonto/postgresql-partition-manager/internal/infra/build
BUILD_DATE=$(shell date -u +'%Y-%m-%dT%H:%M:%SZ')
GIT_COMMIT_SHA=$(shell git rev-parse HEAD)
BINARY=postgresql-partition-manager
COVER_BINARY=test-postgresql-partition-manager
ARCHITECTURE=$(shell uname -m)
HELM_CHART_NAME=postgresql-partition-manager-chart
RELEASE_VERSION=$(shell jq .tag dist/metadata.json)
AWS_ECR_PUBLIC_ORGANIZATION=qonto

all: build

.PHONY: format
format:
	gofumpt -l -w .

.PHONY: build
build:
	CGO_ENABLED=0 go build -v -ldflags="-X '$(BUILD_INFO_PACKAGE_PATH).Version=development' -X '$(BUILD_INFO_PACKAGE_PATH).CommitSHA=$(GIT_COMMIT_SHA)' -X '$(BUILD_INFO_PACKAGE_PATH).Date=$(BUILD_DATE)'" -o $(BINARY)

coverage: $(COVER_BINARY)
	test -d ./coverage-data || mkdir ./coverage-data
	rm -f ./coverage-data/cov*
# run tests with the testing package
	go test -v ./... -cover -test.gocoverdir=$(PWD)/coverage-data -covermode set
# run e2e test collecting coverage data
	cd scripts/bats && GOCOVERDIR=$(PWD)/coverage-data bats *.bats
# merge the collected traces
	go tool covdata textfmt -i=./coverage-data -o coverage.txt
	go run github.com/boumenot/gocover-cobertura@latest < coverage.txt > coverage.xml
	go tool cover -html coverage.txt -o cover.html


# build for coverage
$(COVER_BINARY): build
	CGO_ENABLED=0 go build -cover -v -ldflags="-X '$(BUILD_INFO_PACKAGE_PATH).Version=development' -X '$(BUILD_INFO_PACKAGE_PATH).CommitSHA=$(GIT_COMMIT_SHA)' -X '$(BUILD_INFO_PACKAGE_PATH).Date=$(BUILD_DATE)'" -o $(COVER_BINARY)

.PHONY: run
run:
	./$(BINARY) $(args)

.PHONY: install
install: build
	GOBIN=/usr/local/bin/ go install -v -ldflags="-X '$(BUILD_INFO_PACKAGE_PATH).Version=development' -X '$(BUILD_INFO_PACKAGE_PATH).CommitSHA=$(GIT_COMMIT_SHA)' -X '$(BUILD_INFO_PACKAGE_PATH).Date=$(BUILD_DATE)'"

.PHONY: bats-test
bats-test: build $(COVER_BINARY)
	test -d ./coverage-data || mkdir ./coverage-data
	cd scripts/bats && GOCOVERDIR=$(PWD)/coverage-data bats *.bats

.PHONY: helm-test
helm-test:
	helm unittest configs/helm

.PHONY: helm-release
helm-release:
	./scripts/helm-release.sh $(HELM_CHART_NAME) configs/helm $(RELEASE_VERSION) $(AWS_ECR_PUBLIC_ORGANIZATION)

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

checkcov:
	checkov --directory .

.PHONY: test
test:
	test -d ./coverage-data || mkdir ./coverage-data
	go test -v ./... -cover -test.gocoverdir=$(PWD)/coverage-data -covermode set

.PHONY: lint
lint:
	golangci-lint run --verbose --timeout 2m

.PHONY: all-tests
all-tests: test helm-test kubeconform-test goreleaser-check
