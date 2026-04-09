FROM golang:1.23 AS builder
ARG GOPROXY=direct

WORKDIR /build
COPY go.mod go.sum ./
RUN go env -w GOPROXY=$GOPROXY
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-X 'github.com/qonto/postgresql-partition-manager/internal/infra/build.Version=development'" -o /test-postgresql-partition-manager

FROM bats/bats:1.11.0

RUN apk add postgresql-client yq

COPY --from=builder /test-postgresql-partition-manager /test-postgresql-partition-manager

RUN adduser -D app

USER app
