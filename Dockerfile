# syntax=docker/dockerfile:1

# ---- build: fully static binary -------------------------------------------------
# Runs on the build host's platform and cross-compiles for the target, so
# multi-arch builds need no QEMU emulation.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS builder
ARG TARGETOS TARGETARCH
RUN apk add --no-cache ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Release version embedded in the binary (`qbo-bq version`). No -trimpath: it
# drops -ldflags from the embedded build info, which is where syft/grype read
# the version from. Paths here are fixed (/src, /go), so nothing leaks.
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -ldflags="-s -w -X main.version=${VERSION}" -o /out/qbo-bq ./cmd/qbo-bq

# ---- runtime: nothing but the binary and CA roots -------------------------------
FROM scratch
# TLS verification against Intuit and Google APIs.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/qbo-bq /qbo-bq
# Time zones are embedded in the binary (import _ "time/tzdata").
USER 65532:65532
ENTRYPOINT ["/qbo-bq"]
