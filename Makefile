# Makefile for building, testing, and profiling Steda.
.DEFAULT_GOAL := help

# List of features to use when building. Can be overridden via the environment.
FEATURES ?=

# Cargo profile for builds.
PROFILE ?= dev

# Number of generated PostgreSQL histories in one stateful run.
STATEFUL_CASES ?= 64

# Minimum number of operations generated per PostgreSQL history.
STATEFUL_MIN_STEPS ?= 32

# Maximum number of operations generated per PostgreSQL history.
STATEFUL_STEPS ?= 96

# Optional deterministic Proptest seed for reproducing a campaign.
STATEFUL_SEED ?=

# Set to 1 to print a human-readable outcome for every generated operation.
STATEFUL_TRACE ?=

##@ Help

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Build

.PHONY: build
build: ## Build Steda into `target` directory.
	cargo build \
		--features "$(FEATURES)" \
		--profile "$(PROFILE)" \
		--locked

##@ Test

.PHONY: test-unit
test-unit: ## Run unit and integration tests, excluding generated PostgreSQL histories.
	cargo nextest run \
		--workspace \
		--all-features \
		-E 'not binary(stateful)' \
		--no-fail-fast \
		--locked

.PHONY: test-stateful
test-stateful: ## Run generated histories against a real PostgreSQL database.
	$(if $(STATEFUL_SEED),PROPTEST_RNG_SEED="$(STATEFUL_SEED)" )\
	$(if $(STATEFUL_TRACE),STEDA_STATEFUL_TRACE="$(STATEFUL_TRACE)" )\
	STEDA_STATEFUL_CASES="$(STATEFUL_CASES)" \
	STEDA_STATEFUL_MIN_STEPS="$(STATEFUL_MIN_STEPS)" \
	STEDA_STATEFUL_STEPS="$(STATEFUL_STEPS)" cargo nextest run \
		--workspace \
		--all-features \
		-E 'binary(stateful)' \
		--no-capture \
		--no-fail-fast \
		--locked

.PHONY: test-doc
test-doc: ## Run doc tests.
	cargo test \
		--doc \
		--workspace \
		--all-features \
		--locked

.PHONY: test-examples
test-examples: ## Build and run all runnable examples.
	@test -n "$${DATABASE_URL:-}" || { \
		echo "DATABASE_URL must be set to run the PostgreSQL-backed examples." >&2; \
		exit 1; \
	}
	cargo build \
		--examples \
		--all-features \
		--locked
	@set -eu; \
	examples='$(sort $(patsubst examples/%.rs,%,$(wildcard examples/*.rs)))'; \
	for example in $$examples; do \
		printf "\n==> Running example: %s\n" "$$example"; \
		cargo run \
			--quiet \
			--example "$$example" \
			--all-features \
			--locked; \
	done

.PHONY: test
test: ## Run deterministic, example, stateful, and documentation tests.
	$(MAKE) test-unit && \
	$(MAKE) test-examples && \
	$(MAKE) test-stateful && \
	$(MAKE) test-doc

.PHONY: test-coverage
test-coverage: ## Run unit tests with coverage and generate an LCOV report.
	cargo +nightly llvm-cov nextest \
		--workspace \
		--all-features \
		-E 'not binary(stateful)' \
		--lcov \
		--output-path lcov.info \
		--locked

.PHONY: test-coverage-html
test-coverage-html: ## Run unit tests with coverage and generate and open an HTML report.
	cargo +nightly llvm-cov nextest \
		--workspace \
		--all-features \
		-E 'not binary(stateful)' \
		--html \
		--open \
		--locked

##@ Linting

.PHONY: fmt
fmt: ## Run all formatters.
	cargo +nightly fmt --all

.PHONY: lint-clippy
lint-clippy: ## Run clippy on the codebase.
	cargo +nightly clippy \
		--workspace \
		--all-targets \
		--all-features \
		--locked \
		-- -D warnings

.PHONY: lint-clippy-fix
lint-clippy-fix: ## Run clippy on the codebase and fix warnings.
	cargo +nightly clippy \
		--workspace \
		--all-targets \
		--all-features \
		--fix \
		--allow-dirty \
		--allow-staged \
		--locked \
		-- -D warnings

.PHONY: lint-typos
lint-typos: ## Run typos on the codebase.
	@command -v typos >/dev/null || { \
		echo "typos not found. Please install it by running the command 'cargo install typos-cli' or refer to the following link for more information: https://github.com/crate-ci/typos"; \
		exit 1; \
	}
	typos

.PHONY: lint
lint: ## Run all linters.
	$(MAKE) fmt && \
	$(MAKE) lint-clippy && \
	$(MAKE) lint-typos

##@ Documentation

.PHONY: doc
doc: ## Build the documentation.
	RUSTDOCFLAGS="--cfg docsrs -D warnings -Zunstable-options --show-type-layout --generate-link-to-definition" \
		cargo +nightly doc \
			--workspace \
			--all-features \
			--document-private-items \
			--no-deps \
			--locked

##@ Other

.PHONY: lock
lock: ## Update the Cargo.lock file with the current dependencies.
	cargo fetch

.PHONY: clean
clean: ## Clean the project.
	cargo clean

.PHONY: deny
deny: ## Perform a `cargo deny` check.
	cargo deny --locked --all-features check all

.PHONY: about
about: ## Generate the `THIRD_PARTY_NOTICES.md` file.
	cargo about generate -c .github/about.toml -o THIRD_PARTY_NOTICES.md .github/about.hbs --all-features --locked

.PHONY: sql
sql: ## Generate the `sql/steda.sql` file.
	@first=1; \
	for file in sql/migrations/*.sql; do \
		if [ "$$first" -eq 0 ]; then printf '\n'; fi; \
		cat "$$file"; \
		first=0; \
	done > sql/steda.sql

.PHONY: check
check: ## Check all crates and targets.
	cargo hack check --locked --feature-powerset --depth 1

.PHONY: pr
pr: ## Run all checks and tests.
	$(MAKE) deny && \
	$(MAKE) check && \
	$(MAKE) lint && \
	$(MAKE) test && \
	$(MAKE) doc && \
	$(MAKE) about && \
	$(MAKE) sql
