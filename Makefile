APP_VERSION := $(shell awk '$$1 == "version:" { print $$2; }' package.yaml)
LOG_FORMAT := bracket

all: build ## Default target, compile the project.

.PHONY: build
build: ## Build the application executable.
	stack build --fast # the released image will be container instead, so we can build locally with quicker flags

.PHONY: demo
demo: build pull-test-images ## Stand up a temporary container indexing metrics from Fastly
	./demo.sh $(LOG_FORMAT)

.PHONY: run
run: ## Run the program with default values.
	stack exec beefheart -- --log-format $(LOG_FORMAT) --verbose

.PHONY: test
test: pull-es-image ## Run test suite
	stack test --fast

.PHONY: image
image: ## Build a container image
	docker build -t beefheart .

.PHONY: pull-test-images
pull-test-images: pull-es-image pull-kibana-image ## Prefetch Docker images used in testing

.PHONY: pull-es-image
pull-es-image: ## Prefetch Elasticsearch test image
	docker pull docker.elastic.co/elasticsearch/elasticsearch:7.7.0

.PHONY: pull-kibana-image
pull-kibana-image: ## Prefetch Kibana test image
	docker pull docker.elastic.co/kibana/kibana:7.7.0

.PHONY: help
help:  ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
