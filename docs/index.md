# PostgreSQL Partition Manager

PostgreSQL Partition Manager (PPM) is an opinionated tool designed to streamline the management of PostgreSQL partitions. It operates on PostgreSQL's declarative partitioning and does not require any specific PostgreSQL extensions.

## Why PPM?

The key objective of PPM is to simplify the use of PostgreSQL partitions for developers. By providing a secure, non-blocking, and intuitive tool for partition management, developers can manage table schema and indexes using existing tools and ORMs, and leverage PPM to implement date-based and UUIDv7-based partitioning for scaling PostgreSQL systems.

## Features

- **Automatic provisioning** — Create upcoming partitions ahead of time
- **Cleanup management** — Delete or detach outdated partitions
- **Configuration checking** — Verify partitions match expected configuration
- **Lifecycle hooks** — Execute custom actions (shell commands, SQL) before/after partition operations
- **Multiple partition intervals** — Support for daily, weekly, monthly, quarterly, and yearly partitioning
- **Flexible partition keys** — Support for `date`, `timestamp`, `timestamptz`, and `uuid` column types

## Getting Started

Head over to the [Getting Started](getting-started.md) guide to set up PPM in minutes, or explore the [Installation](installation.md) page for all available installation methods.
