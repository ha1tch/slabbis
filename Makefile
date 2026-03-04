# Makefile for github.com/ha1tch/slabbis
#
# Targets:
#   make               — default: vet + test
#   make build         — compile library and server binary
#   make test          — run all tests
#   make test-race     — run all tests with race detector
#   make test-verbose  — run all tests with verbose output
#   make vet           — run go vet
#   make fmt           — format source files
#   make fmt-check     — check formatting without modifying files
#   make release-check — verify VERSION, version.go, and docs/CHANGELOG.md agree
#   make clean         — remove build artefacts
#   make help          — print this message

PACKAGE   := github.com/ha1tch/slabbis
BINARY    := slabbis
BUILD_DIR := bin

GO        := go
GOFLAGS   :=

.PHONY: all build test test-race test-verbose vet fmt fmt-check \
        release-check clean help

all: vet test

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

build:
	$(GO) build $(GOFLAGS) ./...
	mkdir -p $(BUILD_DIR)
	$(GO) build $(GOFLAGS) -o $(BUILD_DIR)/$(BINARY) ./cmd/slabbis

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

test:
	$(GO) test $(GOFLAGS) -count=1 ./...

test-race:
	$(GO) test $(GOFLAGS) -race -count=1 ./...

test-verbose:
	$(GO) test $(GOFLAGS) -v -count=1 ./...

# ---------------------------------------------------------------------------
# Code quality
# ---------------------------------------------------------------------------

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

fmt-check:
	@out=$$(gofmt -l .); \
	if [ -n "$$out" ]; then \
	    echo "Files not formatted:"; \
	    echo "$$out"; \
	    exit 1; \
	fi

# ---------------------------------------------------------------------------
# Release hygiene
#
# Verifies that VERSION, version.go, and the top entry in docs/CHANGELOG.md
# all agree on the current version string.
# Run before cutting any tag or zip checkpoint.
# ---------------------------------------------------------------------------

release-check:
	@python3 scripts/release-check.py

# ---------------------------------------------------------------------------
# Clean
# ---------------------------------------------------------------------------

clean:
	$(GO) clean ./...
	rm -rf $(BUILD_DIR)

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------

help:
	@grep -E '^#   make' Makefile | sed 's/^# //'