FROM golang:1.22.5-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/quakewatch-server ./cmd/server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system quakewatch \
    && useradd --system --gid quakewatch --home-dir /app quakewatch \
    && mkdir -p /app /data \
    && chown -R quakewatch:quakewatch /app /data
WORKDIR /app
COPY --from=build /out/quakewatch-server /app/quakewatch-server
USER quakewatch
ENV QUAKEWATCH_HTTP_ADDR=:8080
ENV QUAKEWATCH_DATABASE_PATH=/data/quakewatch.db
EXPOSE 8080
VOLUME ["/data"]
HEALTHCHECK --interval=5s --timeout=2s --start-period=5s --retries=12 CMD curl --fail --silent http://127.0.0.1:8080/healthz || exit 1
ENTRYPOINT ["/app/quakewatch-server"]
