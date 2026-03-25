IMAGE_REPO := us-west1-docker.pkg.dev/sentio-352722/sentio/clickhouse-proxy
TAG ?= $(shell git rev-parse --short HEAD)
IMAGE := $(IMAGE_REPO):$(TAG)

.PHONY: build test docker push update-yaml apply

all: build docker push update-yaml

build:
	bazel build //cmd/proxy:clickhouse-proxy

test:
	bazel test //pkg/proxy:proxy_test

docker:
	bazel build --platforms=@rules_go//go/toolchain:linux_amd64 --@rules_go//go/config:static=true //cmd/proxy:clickhouse-proxy
	cp -f bazel-bin/cmd/proxy/clickhouse-proxy_/clickhouse-proxy clickhouse-proxy
	docker build -f deploy/Dockerfile -t $(IMAGE) .
	rm -f clickhouse-proxy

push:
	docker push $(IMAGE)

update-yaml:
	@echo "Updating external k8s configs with image: $(IMAGE)"
	sed -i 's|image: $(IMAGE_REPO):.*|image: $(IMAGE)|' external/production/k8s-sea/clickhouse/test-clickhouse.yaml
	sed -i 's|image: $(IMAGE_REPO):.*|image: $(IMAGE)|' external/production/k8s-sea/clickhouse/clickhouse-extra.yaml
	sed -i 's|image: $(IMAGE_REPO):.*|image: $(IMAGE)|' external/production/k8s-sea/clickhouse/auth_validate_ck.yaml

apply:
	kubectl apply -f external/production/k8s-sea/clickhouse/test-clickhouse.yaml
	kubectl apply -f external/production/k8s-sea/clickhouse/clickhouse-extra.yaml
	kubectl apply -f external/production/k8s-sea/clickhouse/auth_validate_ck.yaml

# Integration test: stream replay from remote ClickHouse query_log to local proxy
# Usage: make test-stream-replay POD=clickhouse-user-part-a-0-0-0 [NS=clickhouse] [SINCE="1 hour"] [N=0]
NS ?= clickhouse
N ?= 0
SINCE ?= 1 hour

test-stream-replay:
ifndef POD
	@echo "Error: Please specify POD=<pod-name>"
	@echo "Usage: make test-stream-replay POD=clickhouse-user-part-a-0-0-0 [NS=clickhouse] [SINCE='1 hour'] [N=0]"
	@exit 1
endif
	@./tools/run_stream_replay.sh "$(POD)" "$(NS)" "$(SINCE)" "$(N)"

test-forwarding:
	@./tools/run_tests.sh $(N)
