# Makefile for github.com/ha1tch/slabbis
#
# Targets:
#   make               — default: vet + test
#   make build         — compile library and server binary
#   make install       — install server binary to BINDIR (default: ~/.local/bin)
#   make test          — run all tests
#   make test-race     — run all tests with race detector
#   make test-verbose  — run all tests with verbose output
#   make bench         — run all benchmarks (5 s each, 6 runs, cpu=1,4,8)
#   make bench-quick   — run all benchmarks (1 s each, 1 run, cpu=1)
#   make bench-get     — run Get vs GetCopy benchmarks only
#   make vet           — run go vet
#   make fmt           — format source files
#   make fmt-check     — check formatting without modifying files
#   make release-check — verify VERSION, version.go, and CHANGELOG.md agree
#   make clean         — remove build artefacts
#   make help          — print this message

PACKAGE   := github.com/ha1tch/slabbis
BINARY    := slabbis
BUILD_DIR := bin

PREFIX  ?= $(HOME)/.local
BINDIR  ?= $(PREFIX)/bin

GO        := go
GOFLAGS   :=

.PHONY: all build install test test-race test-race-local test-verbose \
        bench bench-quick bench-get \
        vet fmt fmt-check release-check clean help

all: vet test

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

build:
	$(GO) build $(GOFLAGS) ./...
	mkdir -p $(BUILD_DIR)
	$(GO) build $(GOFLAGS) -o $(BUILD_DIR)/$(BINARY) ./cmd/slabbis

# ---------------------------------------------------------------------------
# Install
#
# Installs the server binary to BINDIR.
# Override PREFIX or BINDIR to change the destination:
#   make install PREFIX=/usr/local
#   make install BINDIR=/opt/bin
# ---------------------------------------------------------------------------

install: build
	@mkdir -p $(BINDIR)
	install -m 0755 $(BUILD_DIR)/$(BINARY) $(BINDIR)/$(BINARY)
	@echo "Installed $(BINARY) to $(BINDIR)/$(BINARY)"

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

test:
	$(GO) test $(GOFLAGS) -count=1 ./...

test-race:
	$(GO) test $(GOFLAGS) -race -count=1 ./...

## test-race-local: race detector on, but false positives on Apple Silicon
## do not abort the run. Use locally; CI uses test-race.
test-race-local:
	GORACE="halt_on_error=0" $(GO) test $(GOFLAGS) -race -count=1 ./...

test-verbose:
	$(GO) test $(GOFLAGS) -v -count=1 ./...

# ---------------------------------------------------------------------------
# Benchmarks
#
# bench:       full run — 5 s per benchmark, 6 repetitions, three CPU configs.
#              Pipe through benchstat for a statistically sound comparison:
#                make bench | tee bench.txt
#                benchstat bench.txt          # single-run summary
#                benchstat old.txt new.txt    # compare two runs
#
# bench-quick: fast sanity check — 1 s, single repetition, single CPU.
#              Useful for checking benchmarks compile and produce plausible
#              numbers before committing to a full run.
#
# bench-get:   Get vs GetCopy only — the penalty measurement for the
#              copy-under-lock fix introduced in v0.1.1.
# ---------------------------------------------------------------------------

BENCH_TIME  ?= 5s
BENCH_COUNT ?= 6
BENCH_CPU   ?= 1,4,8

bench:
	$(GO) test $(GOFLAGS) -run ^$$ -bench . \
	    -benchmem -benchtime=$(BENCH_TIME) -count=$(BENCH_COUNT) -cpu=$(BENCH_CPU) .

bench-quick:
	$(GO) test $(GOFLAGS) -run ^$$ -bench . \
	    -benchmem -benchtime=1s -count=1 -cpu=1 .

bench-get:
	$(GO) test $(GOFLAGS) -run ^$$ \
	    -bench 'BenchmarkGet$$|BenchmarkGetCopy$$|BenchmarkGetParallel$$|BenchmarkGetCopyParallel$$|BenchmarkGetMixedParallel$$|BenchmarkGetCopyMixedParallel$$' \
	    -benchmem -benchtime=$(BENCH_TIME) -count=$(BENCH_COUNT) -cpu=$(BENCH_CPU) .

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
# Verifies that VERSION, version.go, and CHANGELOG.md all agree on the
# current version string. Run before cutting any tag or zip checkpoint.
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
