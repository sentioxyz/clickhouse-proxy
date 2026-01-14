IMAGE_REPO := us-west1-docker.pkg.dev/sentio-352722/sentio/clickhouse-proxy
TAG ?= $(shell git rev-parse --short HEAD)
IMAGE := $(IMAGE_REPO):$(TAG)

.PHONY: build docker push update-yaml apply test-forwarding test-stream-replay

all: build docker push update-yaml

build:
	CGO_ENABLED=0 go build -o ck-proxy .

docker:
	docker build -t $(IMAGE) .

push:
	docker push $(IMAGE)

update-yaml:
	@echo "Updating external k8s configs with image: $(IMAGE)"
	sed -i 's|image: $(IMAGE_REPO):.*|image: $(IMAGE)|' external/production/k8s-sea/clickhouse/test-clickhouse.yaml
	sed -i 's|image: $(IMAGE_REPO):.*|image: $(IMAGE)|' external/production/k8s-sea/clickhouse/clickhouse-extra.yaml

apply:
	kubectl apply -f external/production/k8s-sea/clickhouse/test-clickhouse.yaml
	kubectl apply -f external/production/k8s-sea/clickhouse/clickhouse-extra.yaml

test-forwarding:
	@echo "Running local forwarding integration tests..."
	@./tests/run_tests.sh $(N)

# Stream replay from remote ClickHouse query_log to local proxy
# Starts mock server + proxy + port-forward, runs replay, cleans up on exit/Ctrl-C
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
	@./tests/run_stream_replay.sh "$(POD)" "$(NS)" "$(SINCE)" "$(N)"



