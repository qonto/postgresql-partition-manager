FROM alpine:3

ARG USER=app
ARG HOME=/app

RUN addgroup -g 1001 -S app \
    && adduser --home /app -u 1001 -S app -G app \
    && mkdir -p /app \
    && chown app:app -R /app

WORKDIR $HOME
USER $USER

COPY postgresql-partition-manager /app/

ENTRYPOINT ["/app/postgresql-partition-manager"]
