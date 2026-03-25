# Runtime-only image. Binary is built by Bazel on the host.
# Usage:
#   bazel build --platforms=@rules_go//go/toolchain:linux_amd64 //cmd/proxy:clickhouse-proxy
#   docker build -f deploy/Dockerfile -t clickhouse-proxy .
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

COPY clickhouse-proxy .

ENTRYPOINT ["./clickhouse-proxy"]
CMD ["-config", "/app/config.json"]
