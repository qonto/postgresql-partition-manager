FROM golang:1.22 AS builder

WORKDIR /build

COPY go.mod go.sum ./

RUN go mod download

COPY . .

HEALTHCHECK NONE

RUN make build


FROM alpine:3.19

ARG USER=app
ARG HOME=/app

RUN addgroup -g 1001 -S app \
    && adduser --home /app -u 1001 -S app -G app \
    && mkdir -p /app \
    && chown app:app -R /app

WORKDIR $HOME
USER $USER

COPY --from=builder /build/postgresql-partition-manager $HOME/postgresql-partition-manager

ENTRYPOINT [ "/app/postgresql-partition-manager" ]
