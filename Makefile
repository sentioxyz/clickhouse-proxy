IMAGE_REPO := us-west1-docker.pkg.dev/sentio-352722/sentio/clickhouse-proxy
TAG ?= $(shell git rev-parse --short HEAD)
IMAGE := $(IMAGE_REPO):$(TAG)

.PHONY: build docker push update-yaml all

all: build docker push update-yaml

build:
	CGO_ENABLED=0 go build -o ck-proxy .

docker:
	docker build -t $(IMAGE) .

push:
	docker push $(IMAGE)

update-yaml:
	@echo "Updating k8s statefulsets with image: $(IMAGE)"
	sed -i 's|image: $(IMAGE_REPO):.*|image: $(IMAGE)|' k8s/test-clickhouse-proxy-test-part-a-0/statefulset.yaml
	sed -i 's|image: $(IMAGE_REPO):.*|image: $(IMAGE)|' k8s/test-clickhouse-proxy-test-part-a-1/statefulset.yaml
	sed -i 's|image: $(IMAGE_REPO):.*|image: $(IMAGE)|' k8s/test-clickhouse-proxy-test-part-b-0/statefulset.yaml
