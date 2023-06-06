NAMESPACE = myrtea
APP = sdk
GOOS ?= linux
TARGET_ENV ?= local
PORT ?= 9000
TARGET_PORT ?= 9000

PROJECT_PATH = github.com/myrteametrics/myrtea-sdk

APP_NAME = $(NAMESPACE)-$(APP)
DOCKER_IMAGE = $(NAMESPACE)/$(APP):$(TARGET_ENV)

GO111MODULE ?= on

.DEFAULT_GOAL := help
.PHONY: help
help:
	@echo "===  Myrtea build & deployment helper ==="
	@echo "* Don't forget to check all overridable variables"
	@echo ""
	@echo "The following commands are available :"
	@grep -E '^\.PHONY: [a-zA-Z_-]+.*?## .*$$' $(MAKEFILE_LIST) | cut -c9- | awk 'BEGIN {FS = " ## "}; {printf "\033[36m%-40s\033[0m %s\n", $$1, $$2}'

GO_PACKAGE ?= go list ./... | \
	grep $(PROJECT_PATH)/v4/ | \
	grep -v -e "$(PROJECT_PATH)/v4/docs" | \
	grep -v -e "$(PROJECT_PATH)/v4/tests"

.PHONY: download ## Download all dependencies
download:
	go mod download

.PHONY: test-integration-lw ## Test the code
test-integration-lw:
	go test -p=1 $$($(GO_PACKAGE))

.PHONY: test-integration ## Test the code
test-integration:
	rm -rf coverage
	mkdir -p coverage/unit
	go test -cover -coverpkg=$$($(GO_PACKAGE) | tr '\n' ',') $$($(GO_PACKAGE)) -args -test.gocoverdir="$(PWD)/coverage/unit"
	go tool covdata percent -i=./coverage/unit
	go tool covdata textfmt -i=./coverage/unit -o coverage/profile.out
	go tool cover -html coverage/profile.out -o coverage/coverage.html
	go tool cover -func coverage/profile.out -o coverage/coverage.txt
	cat coverage/coverage.txt

.PHONY: test-unit ## Test the code
test-unit:
	rm -rf coverage
	mkdir -p coverage/unit
	go test -short -cover -coverpkg=$$($(GO_PACKAGE) | tr '\n' ',') $$($(GO_PACKAGE)) -args -test.gocoverdir="$(PWD)/coverage/unit"
	go tool covdata percent -i=./coverage/unit
	go tool covdata textfmt -i=./coverage/unit -o coverage/profile.out
	go tool cover -html coverage/profile.out -o coverage/coverage.html
	go tool cover -func coverage/profile.out -o coverage/coverage.txt
	cat coverage/coverage.txt

.PHONY: test-race
test-race:
	go test -short -race $$(go list ./... | grep -v /vendor/)

.PHONY: test-memory
test-memory:
	go test -msan -short $$(go list ./... | grep -v /vendor/)

$(lint):
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

.PHONY: lint ## Lint the code
lint: $(lint)
	go mod tidy
	golangci-lint run --fast=false

.PHONY: lint-stats
lint-stats: $(lint)
	go mod tidy
	golangci-lint run --fast=false --out-format=json | jq -r '.Issues[] | .FromLinter' | sort | uniq -c | sort -nr
	golangci-lint run --fast=false --out-format=json | jq -r '.Issues[] | .FromLinter' | sort | wc

.PHONY: swag ## Generate swagger documentation
swag:
	swag init -g main.go

.PHONY: build ## Build the executable (linux by default)
build:
	# No build on SDK for now

.PHONY: run ## Run the executable
run:
	# No run on SDK for now

.PHONY: docker-build ## Build the executable and docker image (using multi-stages build)
docker-build:
	docker build -t $(DOCKER_IMAGE) -f Dockerfile .

.PHONY: docker-build-local ## Build the docker image (please ensure you used "make build" before this command)
docker-build-local:
	docker build -t $(DOCKER_IMAGE) -f local.Dockerfile .

.PHONY: docker-run ## Run the docker image
docker-run:
	docker run -d --name $(APP_NAME) -p $(PORT):$(TARGET_PORT) $(DOCKER_IMAGE)

.PHONY: docker-stop ## Stop the docker image
docker-stop:
	docker stop $(APP_NAME)

.PHONY: docker-push ## Push the docker image to hub.docker.com
docker-push:
	docker push $(DOCKER_IMAGE)

.PHONY: docker-save ## Export the docker image to a tar file
docker-save:
	docker save --output $(APP_NAME)-$(TARGET_ENV).tar $(DOCKER_IMAGE)

.PHONY: docker-clean ## Delete the docker image from the local cache
docker-clean:
	docker image rm $(DOCKER_IMAGE) $(docker images -f dangling=true -q) || true