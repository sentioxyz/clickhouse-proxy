# Stage 1: Build stage
FROM golang:1.25-alpine AS builder

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o ck-proxy .

# Stage 2: Runtime stage
FROM alpine:latest

# Install necessary runtime dependencies (if needed)
RUN apk --no-cache add ca-certificates tzdata

# Set working directory
WORKDIR /app

# Copy binary from build stage
COPY --from=builder /build/ck-proxy .

# Use ENTRYPOINT to set executable, CMD provides default arguments
# Configuration file can be provided in the following ways (in priority order):
# 1. Kubernetes ConfigMap mount (recommended for production)
# 2. Volume mount: docker run -v /host/config.json:/app/config.json ck-proxy:latest -config /app/config.json
ENTRYPOINT ["./ck-proxy"]
CMD ["-config", "/app/config.json"]
