# Development

Guides for contributors working on PostgreSQL Partition Manager itself, rather
than operating it.

If you want to *use* PPM, start with [Getting Started](../getting-started.md)
and [Configuration](../configuration.md).

## Getting set up

The repository [`CONTRIBUTING.md`](https://github.com/qonto/postgresql-partition-manager/blob/main/CONTRIBUTING.md)
covers the full local development workflow: building, running the test suites
(unit, Bats, Helm), linting, and the PostgreSQL/Kubernetes dev environments.

Quick reference:

```bash
make build   # Build the PPM binary
make test    # Run unit tests with coverage
make lint    # Run golangci-lint
```

## Topics

| Guide | Description |
|-------|-------------|
| [Developing a Hook Type](hook-types.md) | Add a new hook runner (e.g. `s3`) to the hook engine |

## Documentation

This site is built with [MkDocs](https://www.mkdocs.org/) and the Material
theme. To preview changes locally:

```bash
pip install -r requirements-docs.txt
mkdocs serve
```

Build and validate (treats warnings as errors, matching CI):

```bash
mkdocs build --strict
```

!!! note
    The [CLI Reference](../cli-reference.md) page is auto-generated from the
    Cobra command tree with `make docs-generate`. Never edit it by hand, and
    regenerate it in the same commit as any change to commands or flags in
    `cmd/`.
