# 第一阶段：构建阶段
FROM golang:1.25-alpine AS builder

# 设置工作目录
WORKDIR /build

# 复制 go mod 文件
COPY go.mod go.sum ./

# 下载依赖
RUN go mod download

# 复制源代码
COPY . .

# 构建应用
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o ck-proxy .

# 第二阶段：运行阶段
FROM alpine:latest

# 安装必要的运行时依赖（如果需要）
RUN apk --no-cache add ca-certificates tzdata

# 设置工作目录
WORKDIR /app

# 从构建阶段复制二进制文件
COPY --from=builder /build/ck-proxy .

# 使用 ENTRYPOINT 设置可执行文件，CMD 提供默认参数
# 配置文件通过以下方式提供（按优先级）：
# 1. Kubernetes ConfigMap 挂载（推荐用于生产环境）
# 2. Volume 挂载: docker run -v /host/config.json:/app/config.json ck-proxy:latest -config /app/config.json
# 3. 环境变量: docker run -e CK_CONFIG=/path/to/config.json ck-proxy:latest
# 4. 命令行参数: docker run ck-proxy:latest -config /path/to/config.json
ENTRYPOINT ["./ck-proxy"]
CMD ["-config", "/app/config.json"]
