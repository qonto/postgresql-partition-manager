# Release Process

## Overview

The PostgreSQL Partition Manager project has the following components:

- PostgreSQL Partition Manager binary
- Docker image
- Helm chart
- Debian package

## Versioning Strategy

The project is using [Semantic Versioning](https://semver.org):

- MAJOR version may introduce incompatible changes
- MINOR version introduces functionality in a backward compatible manner
- PATCH version introduces backward compatible bug fixes

## Releasing a New Version

The following steps must be done by one of the PostgreSQL Partition Manager Maintainers:

- Verify the CI tests pass before continuing.
- Create a tag using the current `HEAD` of the `main` branch by using `git tag v<major>.<minor>.<patch>`
- Push the tag to upstream using `git push upstream v<major>.<minor>.<patch>`
- This tag will kick-off the [GitHub Release Workflow](https://github.com/qonto/postgresql-partition-manager/blob/main/.github/workflows/release.yaml), which will auto-generate GitHub release with multi-architecture binaries and Debian package, publish new release of amd64/arm64 docker images and Helm chart into the container registry
