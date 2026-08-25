# Copyright 2026 PipeOps and the Portage Authors.
# SPDX-License-Identifier: Apache-2.0

IMG ?= ghcr.io/pipeopshq/portage:dev
# v0.3.x emits apiextensions.k8s.io/v1beta1, which Kind 1.22+ (and CI's 1.35) reject.
CONTROLLER_GEN_VERSION ?= v0.18.0
CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)
GO ?= go

.PHONY: all
all: fmt generate manifests test build

.PHONY: fmt
fmt:
	$(GO) fmt ./...

.PHONY: generate
generate:
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./api/..."

.PHONY: manifests
manifests: generate
	$(CONTROLLER_GEN) crd paths="./api/..." output:crd:artifacts:config=config/crd/bases
	$(CONTROLLER_GEN) rbac:roleName=portage-manager-role paths="./internal/controller/..." output:rbac:dir=config/rbac

.PHONY: build
build: generate
	$(GO) build -o bin/portage ./cmd/portage
	$(GO) build -o bin/controller ./cmd/controller

.PHONY: test
test:
	$(GO) test -race -count=1 ./...

.PHONY: vet
vet:
	$(GO) vet ./...

.PHONY: tidy
tidy:
	$(GO) mod tidy

.PHONY: docker-build
docker-build:
	docker build -t $(IMG) .

.PHONY: docs
docs:
	pip install -q -r requirements-docs.txt
	mkdocs serve -a 127.0.0.1:8000

.PHONY: help
help:
	@echo "fmt generate manifests build test vet tidy docker-build docs"
