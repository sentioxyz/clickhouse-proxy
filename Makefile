IMAGE_REPO := us-west1-docker.pkg.dev/sentio-352722/sentio/clickhouse-proxy
TAG ?= $(shell git rev-parse --short HEAD)
IMAGE := $(IMAGE_REPO):$(TAG)

.PHONY: build docker push update-yaml apply check all

all: build docker push update-yaml apply

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

apply:
	kubectl apply -f k8s/test-clickhouse-proxy-test-part-a-0/statefulset.yaml
	kubectl apply -f k8s/test-clickhouse-proxy-test-part-a-1/statefulset.yaml
	kubectl apply -f k8s/test-clickhouse-proxy-test-part-b-0/statefulset.yaml

check:
	@echo "Checking images for statefulsets to ensure they match: $(IMAGE)"
	@failed=0; \
	for sts in test-clickhouse-proxy-test-part-a-0 test-clickhouse-proxy-test-part-a-1 test-clickhouse-proxy-test-part-b-0; do \
		current_image=$$(kubectl get statefulset -n clickhouse $$sts -o jsonpath='{.spec.template.spec.containers[0].image}'); \
		if [ "$$current_image" = "$(IMAGE)" ]; then \
			echo "✅ $$sts: MATCH"; \
		else \
			echo "❌ $$sts: MISMATCH"; \
			echo "   Expected: $(IMAGE)"; \
			echo "   Got:      $$current_image"; \
			failed=1; \
		fi \
	done; \
	if [ $$failed -eq 0 ]; then \
		echo "🎉 All statefulsets updated successfully!"; \
	else \
		echo "⚠️  Update verification failed!"; \
		exit 1; \
	fi
