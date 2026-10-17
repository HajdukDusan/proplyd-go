GO      ?= GOTOOLCHAIN=local go
PKGS    := ./...

.PHONY: all test test-short stress badnet chaos lint tidy

all: lint test

# Everything (unit, integration, stress, bad network, chaos, replicas) with
# the race detector. Needs redis-server on PATH (or PROPLYD_REDIS_SERVER).
test:
	$(GO) test -race -count=1 $(PKGS)

# Shorter stress/chaos runs.
test-short:
	$(GO) test -short -race -count=1 $(PKGS)

stress:
	PROPLYD_STRESS_DURATION=30s PROPLYD_STRESS_EDGES=48 $(GO) test -race -count=1 -run TestStress -v ./tests/

badnet:
	PROPLYD_BADNET_DURATION=30s $(GO) test -race -count=1 -run TestBadNetwork -v ./tests/

chaos:
	PROPLYD_CHAOS_DURATION=60s $(GO) test -race -count=1 -run 'TestChaos|TestReplicas' -v ./tests/

lint:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)
	$(GO) vet $(PKGS)
	staticcheck $(PKGS)

tidy:
	$(GO) mod tidy
