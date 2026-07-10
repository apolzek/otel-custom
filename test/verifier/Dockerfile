# Build context is this directory (test/verifier).
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY main.go ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -o /verifier .

FROM alpine:3.21
COPY --from=build /verifier /verifier
ENTRYPOINT ["/verifier"]
