# AGENTS.md

Instructions for AI coding agents working on this repository.

## Setup

- `make build` — Build the PPM binary (static, CGO_ENABLED=0)
- `go mod download` — Download Go module dependencies

## Test

- `make test` — Run unit tests with coverage
- `make lint` — Run golangci-lint (configured via `.golangci.yml`)
- `make helm-test` — Run Helm chart unit tests
- `make all-tests` — Run all tests (unit + helm + kubeconform + goreleaser check)

## Documentation

- `pip install -r requirements-docs.txt` — Install MkDocs dependencies
- `make docs-generate` — Generate CLI reference from Cobra command tree
- `mkdocs serve` — Preview documentation locally at http://127.0.0.1:8000
- `mkdocs build --strict` — Build and validate documentation (fails on warnings)

## Rules

- When adding or modifying CLI commands or flags in `cmd/`, always run `make docs-generate` to regenerate `docs/cli-reference.md`
- Never edit `docs/cli-reference.md` manually — it is auto-generated from the Cobra command tree
- Include the regenerated `docs/cli-reference.md` in the same commit as the CLI change
- Documentation must build without warnings (`mkdocs build --strict`)
