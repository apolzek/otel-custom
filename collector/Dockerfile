# Build context must be the repository root (see test/docker-compose.yml).
FROM golang:1.25-bookworm AS build
RUN go install go.opentelemetry.io/collector/cmd/builder@v0.156.0
WORKDIR /src
COPY components/ components/
COPY collector/builder-config.yaml collector/
WORKDIR /src/collector
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    /go/bin/builder --config builder-config.yaml

FROM alpine:3.21
RUN addgroup -S otel && adduser -S otel -G otel
COPY --from=build /src/collector/_build/otelcol-custom /otelcol-custom
COPY collector/config.yaml /etc/otelcol/config.yaml
USER otel
EXPOSE 4317 4318 13133
ENTRYPOINT ["/otelcol-custom"]
CMD ["--config", "/etc/otelcol/config.yaml"]
