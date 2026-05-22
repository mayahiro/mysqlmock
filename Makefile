GO ?= go

PKGS := ./...
BIN_DIR ?= bin
BIN ?= $(BIN_DIR)/mysqlmock

.PHONY: fmt vet test build clean

fmt:
	$(GO) tool goimports -local github.com/mayahiro/mysqlmock -l -w .

vet:
	$(GO) vet $(PKGS)

test:
	$(GO) test $(PKGS)

build:
	mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN) ./cmd/mysqlmock

clean:
	rm -rf $(BIN_DIR)
