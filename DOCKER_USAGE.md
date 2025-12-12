# Docker Usage Guide

This document describes how to build and run ClickHouse Proxy using Docker.

## Table of Contents

- [Building Images](#building-images)
- [Running Containers](#running-containers)
- [Image Information](#image-information)

## Building Images

### Local Build

```bash
# Execute in the project root directory
docker build -t clickhouse-proxy:latest .
```

### Build with Specific Tag

```bash
docker build -t clickhouse-proxy:v0.1.0 .
```

### Build to Private Registry

```bash
# Build and tag
docker build -t ${}/clickhouse-proxy:v0.1.0 .

# Push to registry
docker push us-west1-docker.pkg.dev/sentio-352722/sentio/clickhouse-proxy:v0.1.0
```

## Running Containers

### Basic Run

Using the default config path `/app/config.json`:

```bash
docker run -d \
  --name clickhouse-proxy \
  -p 9000:9000 \
  clickhouse-proxy:latest
```

### Mount Configuration File

Recommended approach: mount the configuration file via volume

```bash
docker run -d \
  --name clickhouse-proxy \
  -p 9000:9000 \
  -v /path/to/config.json:/app/config.json \
  clickhouse-proxy:latest \
  -config /app/config.json
```

### Configuration File Example

Create `config.json`:

```json
{
  "listen": "0.0.0.0:9000",
  "upstream": "clickhouse-server:9000",
  "stats_interval": "30s",
  "dial_timeout": "5s",
  "idle_timeout": "5m",
  "log_queries": true,
  "log_data": false,
  "max_query_log_bytes": 300,
  "max_data_log_bytes": 200
}
```

### Configuration Parameters

- `listen`: Proxy listening address and port (default: `:9000`)
- `upstream`: Upstream ClickHouse server address and port
- `stats_interval`: Statistics output interval (default: `10s`)
- `dial_timeout`: Upstream connection timeout (default: `5s`)
- `idle_timeout`: Idle connection timeout (default: `5m`)
- `log_queries`: Whether to log queries (default: `true`)
- `log_data`: Whether to log data packets (default: `false`)
- `max_query_log_bytes`: Maximum bytes for query logs (default: `300`)
- `max_data_log_bytes`: Maximum bytes for data logs (default: `200`)

## Image Information

### Base Images

- **Build stage**: `golang:1.25-alpine`
- **Runtime stage**: `alpine:latest`

### Runtime Dependencies

The image includes the following runtime dependencies:
- `ca-certificates`: SSL/TLS certificates
- `tzdata`: Timezone data

### Working Directory

Container working directory: `/app`

### Binary File

Built binary file: `/app/ck-proxy`

### Testing Connection

```bash
# Test using ClickHouse client
clickhouse-client --host localhost --port 9000
```

## Notes

1. Ensure the configuration file path is correct; the default path inside the container is `/app/config.json`
2. If using volume mount, ensure the configuration file exists on the host with proper permissions
3. Avoid port conflicts with other services on the host when mapping ports
4. For production environments, it's recommended to use Kubernetes ConfigMap for configuration management
5. Ensure the upstream ClickHouse server address is accessible
