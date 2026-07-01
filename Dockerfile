# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM golang:latest AS builder

WORKDIR /app

# Cache dependencies before copying the full source (layer caching).
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a statically linked binary.
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o s3rudder .

# ── Stage 2: minimal runtime image ────────────────────────────────────────────
FROM gcr.io/distroless/static:nonroot

WORKDIR /app
COPY --from=builder /app/s3rudder .

EXPOSE 8080

ENTRYPOINT ["/app/s3rudder"]
CMD ["-config", "/etc/s3rudder/config.yaml"]
