# Runtime stage
FROM alpine:latest

# Install necessary runtime dependencies (if needed)
RUN apk --no-cache add ca-certificates tzdata

# Set working directory
WORKDIR /app

# Copy binary from build context (assumes built locally)
COPY ck-proxy .

# Use ENTRYPOINT to set executable, CMD provides default arguments
# Configuration file can be provided in the following ways (in priority order):
# 1. Kubernetes ConfigMap mount (recommended for production)
# 2. Volume mount: docker run -v /host/config.json:/app/config.json ck-proxy:latest -config /app/config.json
ENTRYPOINT ["./ck-proxy"]
CMD ["-config", "/app/config.json"]
