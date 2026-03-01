# Distributed KV Node - Makefile

CLUSTER_SIZE ?= 3
HOST ?= 127.0.0.1
BASE_PORT ?= 8200
MAX_NODES ?= 32
MVN := mvn -q
BUILD_DIR := build
JVM_OPTS := -XX:+UseZGC -Xms256m -Xmx256m
JVM_OPTS_PROFILE := $(JVM_OPTS) -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints
WAIT_TIMEOUT_SECONDS := 30

.PHONY: help build package run run-node stop stop-node clean load-test test verify flamegraph-deps run-flamegraph-node1 flamegraph flamegraph-run wait-node-up check-nodes-up doctor lint-check lint-fix

# Prints targets and examples; no side effects.
help:
	@echo "Distributed KV Node"
	@echo ""
	@echo "Targets:"
	@echo "  build, package, test, verify, clean"
	@echo "  run [CLUSTER_SIZE=3]  start cluster (1..$(MAX_NODES) nodes)"
	@echo "  run-node NODE=nodei [CLUSTER_SIZE=k]  start node i of k (default k=15)"
	@echo "  stop, stop-node NODE=nodei"
	@echo "  load-test [CLUSTER_SIZE=3]  run then vegeta load test"
	@echo "  flamegraph-deps   install async-profiler + FlameGraph (Homebrew)"
	@echo "  run-flamegraph-node1  start node 1 for profiling"
	@echo "  flamegraph        capture 60s CPU profile -> build/flamegraph.svg"
	@echo "  flamegraph-run    one-shot: run node, load, capture, stop"
	@echo "  doctor            check required and optional dependencies"
	@echo "  lint-check        run Java linters (Checkstyle, format check; SpotBugs skipped for Java 25)"
	@echo "  lint-fix         auto-fix formatting (Eclipse formatter)"
	@echo ""
	@echo "Examples:"
	@echo "  make run CLUSTER_SIZE=5"
	@echo "  make run-node NODE=node2 CLUSTER_SIZE=5"
	@echo "  make stop-node NODE=node1"
	@echo "  make load-test RATE=100 DURATION=30s"
	@echo "  make load-test CLUSTER_SIZE=15 BASE_URL=http://127.0.0.1:8201"
	@echo "  make flamegraph-deps && make run-flamegraph-node1 && make flamegraph"
	@echo "  make doctor"
	@echo ""
	@echo "Overrides (defaults): HOST=127.0.0.1 BASE_PORT=8200 MAX_NODES=32; quorum default min(3, cluster size)"
	@echo "  make run HOST=0.0.0.0 BASE_PORT=8300"
	@echo "  make run CLUSTER_SIZE=7 WRITE_QUORUM=2 READ_QUORUM=2"
	@echo "  make run-node NODE=node2 CLUSTER_SIZE=5 WRITE_QUORUM=2 READ_QUORUM=2"
	@echo "  make wait-node-up URL=http://127.0.0.1:8201/health"

# Compiles only; no JAR produced.
build:
	$(MVN) compile

# Produces target/kv-node-*.jar; required by run, run-node, load-test, flamegraph targets.
package:
	$(MVN) package

# Starts cluster of CLUSTER_SIZE nodes (default 3, 1..MAX_NODES). Validates 1 <= CLUSTER_SIZE <= MAX_NODES; stops any existing nodes first. Quorum default min(3, CLUSTER_SIZE); override with WRITE_QUORUM, READ_QUORUM. Waits for each node /health before returning.
run: package
	@mkdir -p $(BUILD_DIR)
	@$(MAKE) stop
	@n="$(CLUSTER_SIZE)"; n=$${n:-3}; max=$(MAX_NODES); \
	if [ "$$n" -lt 1 ] 2>/dev/null || [ "$$n" -gt "$$max" ] 2>/dev/null; then echo "CLUSTER_SIZE must be 1..$$max"; exit 1; fi; \
	jar_path=$$(ls target/kv-node-*.jar 2>/dev/null | head -n 1); \
	if [ -z "$$jar_path" ]; then echo "JAR not found. Run make package."; exit 1; fi; \
	quorum=$$(( n > 3 ? 3 : n )); wq="$(WRITE_QUORUM)"; rq="$(READ_QUORUM)"; wq=$${wq:-$$quorum}; rq=$${rq:-$$quorum}; \
	nodes=""; for i in $$(seq 1 $$n); do p=$$(($(BASE_PORT) + i)); nodes="$$nodeshttp://$(HOST):$$p,"; done; nodes=$${nodes%,}; \
	for i in $$(seq 1 $$n); do \
		port=$$(($(BASE_PORT) + i)); \
		echo "Starting node $$i (port $$port)..."; \
		PORT=$$port NODE_URL=http://$(HOST):$$port NODES=$$nodes REPLICATION_FACTOR=$$n WRITE_QUORUM=$$wq READ_QUORUM=$$rq java $(JVM_OPTS) -jar "$$jar_path" > $(BUILD_DIR)/node$$i.log 2>&1 & echo $$! > $(BUILD_DIR)/node$$i.pid; \
	done; \
	for i in $$(seq 1 $$n); do port=$$(($(BASE_PORT) + i)); $(MAKE) wait-node-up URL=http://$(HOST):$$port/health; done; \
	echo "All $$n nodes started. Use 'make stop' to stop."

# Starts single node. NODE=nodei (e.g. node1, node2) required; CLUSTER_SIZE = cluster size (default 15, max MAX_NODES). Stops that node if already running. NODES list built for 1..CLUSTER_SIZE; use same CLUSTER_SIZE for every node in the cluster.
run-node: package
	@if [ -z "$(NODE)" ]; then echo "NODE is required. Example: make run-node NODE=node1 CLUSTER_SIZE=5"; exit 1; fi; \
	i=$$(echo "$(NODE)" | sed 's/node//'); \
	if [ -z "$$i" ] || ! [ "$$i" -ge 1 ] 2>/dev/null; then echo "NODE must be node1, node2, ..."; exit 1; fi; \
	cs="$(CLUSTER_SIZE)"; cs=$${cs:-15}; max=$(MAX_NODES); \
	if [ "$$cs" -lt 1 ] 2>/dev/null || [ "$$cs" -gt "$$max" ] 2>/dev/null; then echo "CLUSTER_SIZE must be 1..$$max"; exit 1; fi; \
	if [ "$$i" -lt 1 ] 2>/dev/null || [ "$$i" -gt "$$cs" ] 2>/dev/null; then echo "NODE $(NODE) index $$i must be 1..$$cs"; exit 1; fi; \
	port=$$(($(BASE_PORT) + i)); \
	mkdir -p $(BUILD_DIR); \
	$(MAKE) stop-node NODE=$(NODE) 2>/dev/null || true; \
	jar_path=$$(ls target/kv-node-*.jar 2>/dev/null | head -n 1); \
	if [ -z "$$jar_path" ]; then echo "JAR not found. Run make package."; exit 1; fi; \
	quorum=$$(( cs > 3 ? 3 : cs )); wq="$(WRITE_QUORUM)"; rq="$(READ_QUORUM)"; wq=$${wq:-$$quorum}; rq=$${rq:-$$quorum}; \
	nodes=""; for k in $$(seq 1 $$cs); do p=$$(($(BASE_PORT) + k)); nodes="$$nodeshttp://$(HOST):$$p,"; done; nodes=$${nodes%,}; \
	PORT=$$port NODE_URL=http://$(HOST):$$port NODES=$$nodes REPLICATION_FACTOR=$$cs WRITE_QUORUM=$$wq READ_QUORUM=$$rq java $(JVM_OPTS) -jar "$$jar_path" > $(BUILD_DIR)/node$$i.log 2>&1 & echo $$! > $(BUILD_DIR)/node$$i.pid; \
	$(MAKE) wait-node-up URL=http://$(HOST):$$port/health; \
	echo "Node $$i started (port $$port). PID: $$(cat $(BUILD_DIR)/node$$i.pid)"

# Kills all processes whose PIDs are in build/node*.pid; retries then SIGKILL. Removes pid files. Safe if build/ or pid files missing.
stop:
	@if [ -d $(BUILD_DIR) ]; then \
		for f in $(BUILD_DIR)/node*.pid; do \
			if [ -f "$$f" ]; then \
				pid=$$(cat $$f 2>/dev/null); \
				if [ -n "$$pid" ] && kill -0 $$pid 2>/dev/null; then \
					echo "Stopping process $$pid ($$f)..."; \
					kill $$pid 2>/dev/null || true; \
					for j in $$(seq 1 20); do \
						if ! kill -0 $$pid 2>/dev/null; then break; fi; \
						sleep 0.25; \
					done; \
					if kill -0 $$pid 2>/dev/null; then kill -9 $$pid 2>/dev/null || true; fi; \
				fi; \
				rm -f $$f; \
			fi; \
		done; \
		echo "Stopped."; \
	else \
		echo "No $(BUILD_DIR) or PID files found."; \
	fi

# Stops one node. Requires NODE=nodei (e.g. node1). Fails if NODE not set.
stop-node:
	@if [ -z "$(NODE)" ]; then echo "NODE is required. Example: make stop-node NODE=node1"; exit 1; fi
	@f="$(BUILD_DIR)/$(NODE).pid"; \
	if [ -f "$$f" ]; then \
		pid=$$(cat "$$f" 2>/dev/null); \
		if [ -n "$$pid" ] && kill -0 $$pid 2>/dev/null; then \
			echo "Stopping process $$pid ($$f)..."; \
			kill $$pid 2>/dev/null || true; \
			for i in $$(seq 1 20); do \
				if ! kill -0 $$pid 2>/dev/null; then break; fi; \
				sleep 0.25; \
			done; \
			if kill -0 $$pid 2>/dev/null; then kill -9 $$pid 2>/dev/null || true; fi; \
		fi; \
		rm -f "$$f"; \
	fi

# Depends on stop; then mvn clean and deletes build/ entirely.
clean: stop
	$(MVN) clean
	@rm -rf $(BUILD_DIR)

# Starts nodes via run then invokes load/run.sh. Pass CLUSTER_SIZE to run; BASE_URL, RATE, DURATION, TARGETS, etc. as make vars or env; vegeta required.
load-test: run
	@BASE_URL="$(BASE_URL)" RATE="$(RATE)" DURATION="$(DURATION)" TARGETS="$(TARGETS)" SAVE_RESULTS="$(SAVE_RESULTS)" CONNECTIONS="$(CONNECTIONS)" PHASE_DELAY="$(PHASE_DELAY)" ./load/run.sh

# Unit tests only.
test:
	$(MVN) test

# Runs tests plus security (dependency-check) profile.
verify:
	$(MVN) verify -Psecurity

# Polls URL until HTTP 200 or WAIT_TIMEOUT_SECONDS. Requires URL=... (e.g. http://host:port/health).
wait-node-up:
	@if [ -z "$(URL)" ]; then echo "URL is required. Example: make wait-node-up URL=http://$(HOST):$$(($(BASE_PORT)+1))/health"; exit 1; fi
	@echo "Waiting for $(URL) ..."
	@deadline=$$(( $$(date +%s) + $(WAIT_TIMEOUT_SECONDS) )); \
	while [ $$(date +%s) -lt $$deadline ]; do \
		if curl -fsS --max-time 2 "$(URL)" >/dev/null; then echo "Ready: $(URL)"; exit 0; fi; \
		sleep 0.5; \
	done; \
	echo "Timeout waiting for $(URL)"; exit 1

# Expects CLUSTER_SIZE (default 3) in 1..MAX_NODES; pings each node's /health in order. Exits on first failure.
check-nodes-up:
	@echo "Checking node health endpoints..."
	@n="$(CLUSTER_SIZE)"; n=$${n:-3}; max=$(MAX_NODES); \
	if [ "$$n" -lt 1 ] 2>/dev/null || [ "$$n" -gt "$$max" ] 2>/dev/null; then echo "CLUSTER_SIZE must be 1..$$max"; exit 1; fi; \
	nodes=""; for i in $$(seq 1 $$n); do p=$$(($(BASE_PORT) + i)); nodes="$$nodeshttp://$(HOST):$$p,"; done; nodes=$${nodes%,}; \
	for url in $$(echo "$$nodes" | tr ',' ' '); do \
		$(MAKE) wait-node-up URL="$$url/health" >/dev/null || { echo "Node not ready: $$url/health"; exit 1; }; \
	done
	@echo "All nodes are up."

# Checks required and optional dependencies. Exits 1 if any required tool is missing.
doctor:
	@echo "Checking dependencies..."
	@fail=0; \
	check() { if command -v "$$1" >/dev/null 2>&1; then echo "  $$1: OK"; else echo "  $$1: MISSING"; fail=1; fi; }; \
	check_opt() { if command -v "$$1" >/dev/null 2>&1; then echo "  $$1: OK (optional)"; else echo "  $$1: not found (optional, for flamegraph)"; fi; }; \
	echo ""; echo "Required:"; \
	check java; \
	check mvn; \
	check curl; \
	check vegeta; \
	echo ""; echo "Optional (for make flamegraph):"; \
	check_opt asprof; \
	check_opt flamegraph.pl; \
	check_opt brew; \
	echo ""; \
	if [ $$fail -eq 1 ]; then echo "Install missing required tools and run 'make doctor' again."; exit 1; fi; \
	echo "All required dependencies present."

# Run Java linters: Checkstyle, SpotBugs, and format check. Fails if any violation or bug is found.
lint-check: build
	$(MVN) checkstyle:check spotbugs:check formatter:validate

# Auto-fix Java formatting (Eclipse formatter). Modifies source files in place.
lint-fix:
	$(MVN) formatter:format

# Installs async-profiler and FlameGraph via Homebrew (tools/setup-flamegraph.sh). Requires brew.
flamegraph-deps:
	@./tools/setup-flamegraph.sh

# Starts only node 1 with a 3-node cluster config and profiling JVM opts; stops other nodes first. For use with flamegraph target.
run-flamegraph-node1: package
	@mkdir -p $(BUILD_DIR)
	@$(MAKE) stop
	@nf=3; nodes=""; for k in $$(seq 1 $$nf); do p=$$(($(BASE_PORT) + k)); nodes="$$nodeshttp://$(HOST):$$p,"; done; nodes=$${nodes%,}; \
	port=$$(($(BASE_PORT)+1)); \
	jar_path=$$(ls target/kv-node-*.jar 2>/dev/null | head -n 1); \
	if [ -z "$$jar_path" ]; then echo "JAR not found. Run make package."; exit 1; fi; \
	PORT=$$port NODE_URL=http://$(HOST):$$port NODES=$$nodes REPLICATION_FACTOR=$$nf WRITE_QUORUM=3 READ_QUORUM=3 java $(JVM_OPTS_PROFILE) -jar "$$jar_path" > $(BUILD_DIR)/node1.log 2>&1 & echo $$! > $(BUILD_DIR)/node1.pid
	@$(MAKE) wait-node-up URL=http://$(HOST):$$(($(BASE_PORT)+1))/health
	@echo "Node 1 started. Run 'make flamegraph' to capture CPU profile."

# Ensures flamegraph-deps; then records 60s CPU profile from node1.pid. Requires node running via run-flamegraph-node1; empty profile fails. Output build/flamegraph.svg.
flamegraph: flamegraph-deps
	@if [ ! -f $(BUILD_DIR)/node1.pid ]; then echo "Start node with: make run-flamegraph-node1"; exit 1; fi
	@pid=$$(cat $(BUILD_DIR)/node1.pid); \
	if [ -z "$$pid" ] || ! kill -0 $$pid 2>/dev/null; then echo "Process not running. Start with: make run-flamegraph-node1"; exit 1; fi
	@mkdir -p $(BUILD_DIR)
	@asprof -d 60 -e cpu -o collapsed -f $(BUILD_DIR)/cpu-collapsed.txt $$(cat $(BUILD_DIR)/node1.pid) || true
	@if [ ! -s $(BUILD_DIR)/cpu-collapsed.txt ]; then echo "No profile data. Run load in another terminal."; exit 1; fi
	@flamegraph.pl $(BUILD_DIR)/cpu-collapsed.txt > $(BUILD_DIR)/flamegraph.svg
	@echo "Flamegraph: $(BUILD_DIR)/flamegraph.svg"

# One-shot: install deps, start node 1, run load test, capture 60s profile, stop. Produces build/flamegraph.svg. Load runs in background with fixed RATE/DURATION/TARGETS.
flamegraph-run: flamegraph-deps package
	@mkdir -p $(BUILD_DIR)
	@$(MAKE) stop
	@nf=3; nodes=""; for k in $$(seq 1 $$nf); do p=$$(($(BASE_PORT) + k)); nodes="$$nodeshttp://$(HOST):$$p,"; done; nodes=$${nodes%,}; \
	port=$$(($(BASE_PORT)+1)); \
	jar_path=$$(ls target/kv-node-*.jar 2>/dev/null | head -n 1); \
	if [ -z "$$jar_path" ]; then echo "JAR not found. Run make package."; exit 1; fi; \
	PORT=$$port NODE_URL=http://$(HOST):$$port NODES=$$nodes REPLICATION_FACTOR=$$nf WRITE_QUORUM=3 READ_QUORUM=3 java $(JVM_OPTS_PROFILE) -jar "$$jar_path" > $(BUILD_DIR)/node1.log 2>&1 & echo $$! > $(BUILD_DIR)/node1.pid
	@$(MAKE) wait-node-up URL=http://$(HOST):$$(($(BASE_PORT)+1))/health
	@BASE_URL=http://$(HOST):$$(($(BASE_PORT)+1)) RATE=20 DURATION=45s TARGETS=put PHASE_DELAY=0 ./load/run.sh > $(BUILD_DIR)/load-test.log 2>&1 &
	@sleep 5
	@asprof -d 60 -e cpu -o collapsed -f $(BUILD_DIR)/cpu-collapsed.txt $$(cat $(BUILD_DIR)/node1.pid) || true
	@$(MAKE) stop
	@if [ ! -s $(BUILD_DIR)/cpu-collapsed.txt ]; then echo "No profile data."; exit 1; fi
	@flamegraph.pl $(BUILD_DIR)/cpu-collapsed.txt > $(BUILD_DIR)/flamegraph.svg
	@echo "Flamegraph: $(BUILD_DIR)/flamegraph.svg"
