# syntax=docker/dockerfile:1

ARG GO_VERSION=1.27

FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/... \
    && mkdir -p /out/data

# Static binaries on distroless images, running as the nonroot user (65532).
# /data is created owned by that user so a fresh named volume inherits it.

FROM gcr.io/distroless/static-debian12:nonroot AS controller
COPY --from=build /out/controller /usr/local/bin/controller
COPY --from=build --chown=65532:65532 /out/data /data
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/controller"]

FROM gcr.io/distroless/static-debian12:nonroot AS worker
COPY --from=build /out/worker /usr/local/bin/worker
COPY --from=build --chown=65532:65532 /out/data /data
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/worker"]

FROM gcr.io/distroless/static-debian12:nonroot AS client
COPY --from=build /out/objctl /usr/local/bin/objctl
WORKDIR /files
ENTRYPOINT ["/usr/local/bin/objctl"]
