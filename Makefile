IMG ?= ghcr.io/xol1ren/postgres-backup-operator:latest
RUNNER_IMG ?= ghcr.io/xol1ren/postgres-backup-runner:latest
BIN := bin

.PHONY: all build generate manifests test lint fmt vet docker-build install-crds uninstall-crds run-local deploy tools

all: build

tools: ## Install controller-gen, staticcheck and golangci-lint into ./bin
	GOBIN=$(CURDIR)/$(BIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@v0.16.5
	GOBIN=$(CURDIR)/$(BIN) go install honnef.co/go/tools/cmd/staticcheck@2025.1.1
	GOBIN=$(CURDIR)/$(BIN) go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.64.8

generate: ## Generate deepcopy code
	$(BIN)/controller-gen object paths=./api/...

manifests: ## Generate CRD YAML
	$(BIN)/controller-gen crd paths=./api/... output:crd:artifacts:config=deployments/manifests/crds

build: ## Build manager and runner binaries
	CGO_ENABLED=0 go build -o $(BIN)/manager ./cmd/manager
	CGO_ENABLED=0 go build -o $(BIN)/runner ./cmd/runner

fmt:
	go fmt ./...

vet:
	go vet ./...

test: ## Run unit and fake-client integration tests
	go test -race -v ./...

lint: ## Run staticcheck and golangci-lint
	$(BIN)/staticcheck ./...
	$(BIN)/golangci-lint run ./...

docker-build: ## Build the manager (scratch) and runner images
	docker build -t $(IMG) -f Dockerfile .
	docker build -t $(RUNNER_IMG) -f Dockerfile.runner .

install-crds: ## Install CRDs into the current cluster
	kubectl apply -f deployments/manifests/crds

uninstall-crds:
	kubectl delete -f deployments/manifests/crds

deploy: install-crds ## Deploy RBAC and the manager
	kubectl apply -f deployments/manifests/rbac
	kubectl apply -f deployments/operator-deployment.yaml

run-local: install-crds ## Run the manager against the current kubeconfig context
	go run ./cmd/manager --runner-image=$(RUNNER_IMG)

