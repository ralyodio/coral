.PHONY: install ui-build docker-build rust-checks perf-check
.PHONY: postgres-start postgres-url postgres-stop postgres-clean postgres-test-suite postgres-tests
.PHONY: license-check lint-proto lint-sources fix-sources
.PHONY: docs-generate docs-check schema-generate schema-check

LOCAL_POSTGRES_IMAGE ?= postgres:17
LOCAL_POSTGRES_CONTAINER ?= coral-test-postgres
LOCAL_POSTGRES_PORT ?=
DOCKER_IMAGE ?= coral:local
DOCKER_NO_CACHE ?= 0

install: ui-build
	cargo install --path crates/coral-cli --locked

ui-build:
	npm ci --prefix apps/ui
	npm run build --prefix apps/ui
	test -s apps/ui/dist/index.html

# ----------------------------------------------------------------------------
# Local Coral image build
# ----------------------------------------------------------------------------
# Compiles the current checkout in a native Linux BuildKit stage, then packages
# that binary with the same runtime Dockerfile used by the publishing workflow.
# The image platform follows the Docker daemon (arm64 or amd64), so this works
# without cross-compilers or QEMU on both Apple Silicon and Intel machines.
#
#   make docker-build
#   DOCKER_IMAGE=coral:test make docker-build
#   DOCKER_NO_CACHE=1 make docker-build

docker-build:
	@set -eu; \
	if ! command -v docker >/dev/null 2>&1; then \
	  echo "docker is required to build the Coral image" >&2; \
	  exit 1; \
	fi; \
	if ! docker buildx version >/dev/null 2>&1; then \
	  echo "docker buildx is required to build the Coral image" >&2; \
	  exit 1; \
	fi; \
	daemon_arch=$$(docker info --format '{{.Architecture}}'); \
	case "$$daemon_arch" in \
	  amd64|x86_64) image_arch=amd64 ;; \
	  arm64|aarch64) image_arch=arm64 ;; \
	  *) echo "unsupported Docker architecture: $$daemon_arch" >&2; exit 1 ;; \
	esac; \
	case "$(DOCKER_NO_CACHE)" in \
	  0|false|'') no_cache=0 ;; \
	  1|true) no_cache=1 ;; \
	  *) echo "DOCKER_NO_CACHE must be 0, 1, false, or true" >&2; exit 1 ;; \
	esac; \
	git_sha=$$(git rev-parse --short HEAD); \
	test -n "$$git_sha"; \
	tmpdir=$$(mktemp -d); \
	trap 'rm -rf "$$tmpdir"' EXIT HUP INT TERM; \
	context="$$tmpdir/context"; \
	mkdir -p "$$context/dist/$$image_arch" "$$context/docker"; \
	set -- docker buildx build \
	  --platform "linux/$$image_arch" \
	  --provenance=false \
	  --build-arg "CORAL_GIT_SHA=$$git_sha" \
	  --file docker/Dockerfile.local \
	  --target binary \
	  --output "type=local,dest=$$context/dist/$$image_arch"; \
	if [ "$$no_cache" -eq 1 ]; then set -- "$$@" --no-cache; fi; \
	set -- "$$@" .; \
	echo "Compiling the current checkout for linux/$$image_arch..."; \
	"$$@"; \
	test -x "$$context/dist/$$image_arch/coral"; \
	cp docker/Dockerfile docker/entrypoint.sh "$$context/docker/"; \
	set -- docker buildx build \
	  --platform "linux/$$image_arch" \
	  --provenance=true \
	  --file "$$context/docker/Dockerfile" \
	  --load \
	  --tag "$(DOCKER_IMAGE)"; \
	if [ "$$no_cache" -eq 1 ]; then set -- "$$@" --no-cache; fi; \
	set -- "$$@" "$$context"; \
	echo "Building $(DOCKER_IMAGE) from the local linux/$$image_arch binary..."; \
	"$$@"; \
	echo "Built $(DOCKER_IMAGE)"

rust-checks:
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
	cargo nextest run --workspace --all-targets --all-features --locked --no-fail-fast
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked

perf-check:
	cargo build --locked -p coral-cli --release
	cargo run --locked -p xtask --release -- perf-check --coral-bin target/release/coral

# ----------------------------------------------------------------------------
# Postgres-backed tests
# ----------------------------------------------------------------------------
# postgres-test-suite runs DB-backed coral-app test namespaces and the dedicated
# Postgres integration target against an existing Postgres URL. postgres-tests
# provisions the database first: when CORAL_TEST_POSTGRES_URL is set it uses
# that database, otherwise it starts a Docker Postgres matching CI's major
# version and creates a fresh database inside the reusable container for each
# run. Docker chooses an available localhost port by default; set
# LOCAL_POSTGRES_PORT=55432 if you need a stable host port.
#
#   make postgres-start       # start/wait for local Docker Postgres
#   make postgres-url         # print the local Postgres connection URL
#   make postgres-test-suite  # test against CORAL_TEST_POSTGRES_URL
#   make postgres-tests       # provision locally, then run the suite
#   CORAL_TEST_POSTGRES_URL=... make postgres-tests  # use an existing database
#   make postgres-stop        # stop the local Docker Postgres container
#   make postgres-clean       # remove the local Docker Postgres container

postgres-start:
	@set -eu; \
	if ! command -v docker >/dev/null 2>&1; then \
	  echo "docker is required to start local Postgres"; \
	  exit 1; \
	fi; \
	if docker container inspect "$(LOCAL_POSTGRES_CONTAINER)" >/dev/null 2>&1; then \
	  docker start "$(LOCAL_POSTGRES_CONTAINER)" >/dev/null; \
	else \
	  local_postgres_port="$(LOCAL_POSTGRES_PORT)"; \
	  if [ -n "$$local_postgres_port" ]; then \
	    port_arg="-p 127.0.0.1:$$local_postgres_port:5432"; \
	  else \
	    port_arg="-p 127.0.0.1::5432"; \
	  fi; \
	  docker run -d \
	    --name "$(LOCAL_POSTGRES_CONTAINER)" \
	    -e POSTGRES_PASSWORD=postgres \
	    $$port_arg \
	    "$(LOCAL_POSTGRES_IMAGE)" >/dev/null; \
	fi; \
	for _ in $$(seq 1 60); do \
	  if docker exec "$(LOCAL_POSTGRES_CONTAINER)" pg_isready -U postgres >/dev/null 2>&1; then \
	    host_port=$$(docker port "$(LOCAL_POSTGRES_CONTAINER)" 5432/tcp | sed -n 's/.*:\([0-9][0-9]*\)$$/\1/p' | head -n 1); \
	    if [ -z "$$host_port" ]; then \
	      echo "Could not determine local Postgres host port"; \
	      exit 1; \
	    fi; \
	    echo "Postgres is ready at postgres://postgres:postgres@127.0.0.1:$$host_port/postgres"; \
	    exit 0; \
	  fi; \
	  sleep 1; \
	done; \
	docker logs "$(LOCAL_POSTGRES_CONTAINER)" || true; \
	echo "Postgres did not become ready"; \
	exit 1

postgres-url:
	@set -eu; \
	host_port=$$(docker port "$(LOCAL_POSTGRES_CONTAINER)" 5432/tcp | sed -n 's/.*:\([0-9][0-9]*\)$$/\1/p' | head -n 1); \
	if [ -z "$$host_port" ]; then \
	  echo "Local Postgres is not exposing 5432/tcp. Run make postgres-start first."; \
	  exit 1; \
	fi; \
	echo "postgres://postgres:postgres@127.0.0.1:$$host_port/postgres"

postgres-stop:
	@docker stop "$(LOCAL_POSTGRES_CONTAINER)" >/dev/null 2>&1 || true

postgres-clean:
	@docker rm -f "$(LOCAL_POSTGRES_CONTAINER)" >/dev/null 2>&1 || true

# The Cargo invocations that make up the Postgres suite, defined once so
# postgres-test-suite and postgres-tests share a single source of truth without
# a recursive $(MAKE) call inside a recipe. GNU make runs any recipe line
# containing $(MAKE) even under -n/-t/-q, and the postgres-tests recipe is a
# single continued line, so an inline sub-make would make
# `make -n postgres-tests` execute the real suite.
POSTGRES_SUITE_CARGO = cargo test --locked -p coral-app --lib 'state::db::' -- --test-threads=1 && cargo test --locked -p coral-app --lib 'telemetry::service::tests::' && cargo test --locked -p coral-app --test grpc 'source_lifecycle_tests::' && cargo test --locked -p coral-app --test postgres_database_tests

postgres-test-suite:
	@test -n "$${CORAL_TEST_POSTGRES_URL:-}" || \
	  { echo "CORAL_TEST_POSTGRES_URL is required" >&2; exit 1; }
	$(POSTGRES_SUITE_CARGO)

# Provision the local container through a prerequisite rather than a recursive
# $(MAKE) call inside the recipe, for the reason described above.
# CORAL_TEST_POSTGRES_URL reaches both this conditional and the recipe's shell
# whether it is set in the environment or on the command line.
POSTGRES_TESTS_PREREQS :=
ifeq ($(strip $(CORAL_TEST_POSTGRES_URL)),)
POSTGRES_TESTS_PREREQS := postgres-start
endif

postgres-tests: $(POSTGRES_TESTS_PREREQS)
	@set -eu; \
	url="$${CORAL_TEST_POSTGRES_URL:-}"; \
	cleanup() { :; }; \
	if [ -z "$$url" ]; then \
	  host_port=$$(docker port "$(LOCAL_POSTGRES_CONTAINER)" 5432/tcp | sed -n 's/.*:\([0-9][0-9]*\)$$/\1/p' | head -n 1); \
	  if [ -z "$$host_port" ]; then \
	    echo "Local Postgres is not exposing 5432/tcp"; \
	    exit 1; \
	  fi; \
	  db_name="coral_test_$$(date +%s)_$$$$"; \
	  docker exec "$(LOCAL_POSTGRES_CONTAINER)" createdb -U postgres "$$db_name"; \
	  cleanup() { docker exec "$(LOCAL_POSTGRES_CONTAINER)" dropdb -U postgres --if-exists "$$db_name" >/dev/null 2>&1 || true; }; \
	  url="postgres://postgres:postgres@127.0.0.1:$$host_port/$$db_name"; \
	fi; \
	trap cleanup EXIT INT TERM; \
	echo "Running Postgres tests against $$url"; \
	export CORAL_TEST_POSTGRES_URL="$$url"; \
	$(POSTGRES_SUITE_CARGO)

# ----------------------------------------------------------------------------
# Dependency license scan
# ----------------------------------------------------------------------------
# Fails if any workspace dependency uses a license outside the allow-list in
# deny.toml. Requires `cargo-deny`.
#
#   make license-check

license-check:
	cargo deny --version >/dev/null 2>&1 || cargo install --locked cargo-deny
	cargo deny check licenses

# ----------------------------------------------------------------------------
# Protobuf API linting
# ----------------------------------------------------------------------------
# Lints crates/coral-api/proto with Buf.
#
#   make lint-proto   # check protobuf style and API-shape rules

lint-proto:
	cd crates/coral-api && buf lint

# ----------------------------------------------------------------------------
# Source manifest linting
# ----------------------------------------------------------------------------
# Lints sources/ with ryl (Rust-native yamllint port).
#
#   make lint-sources   # check only — run before pushing changes
#   make fix-sources    # apply ryl's safe auto-fixes in place

lint-sources:
	ryl sources

fix-sources:
	ryl --fix sources

# ----------------------------------------------------------------------------
# Source docs generation
# ----------------------------------------------------------------------------
# Regenerates the source catalog pages and Mintlify navigation from
# sources/core/*/manifest.y{a,}ml and sources/community/*/manifest.y{a,}ml
# via the xtask binary. docs-check intentionally skips the community source
# catalog so PRs do not fail on aggregate community source catalog drift.
#
#   make docs-generate   # write/refresh the generated files in apps/docs/
#   make docs-check      # CI freshness check: non-zero exit if stale

docs-generate:
	cargo run --locked -p xtask -- generate-docs \
	  --sources-dir sources/core \
	  --index apps/docs/reference/bundled-sources.mdx \
	  --community-sources-dir sources/community \
	  --community-index apps/docs/reference/community-sources.mdx \
	  --docs-json apps/docs/docs.json

docs-check:
	cargo run --locked -p xtask -- generate-docs \
	  --sources-dir sources/core \
	  --index apps/docs/reference/bundled-sources.mdx \
	  --docs-json apps/docs/docs.json \
	  --skip-community-sources \
	  --check

# ----------------------------------------------------------------------------
# JSON schema generation
# ----------------------------------------------------------------------------
# Regenerates manifest schemas that are generated from Rust types.
#
#   make schema-generate   # write/refresh generated schemas
#   make schema-check      # CI freshness check: non-zero exit if stale

schema-generate:
	cargo run --locked -p xtask -- generate-schemas

schema-check:
	cargo run --locked -p xtask -- generate-schemas --check
