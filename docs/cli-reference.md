# CLI Reference

<!-- This file is auto-generated from the Cobra command tree. Do not edit manually. -->

## postgresql-partition-manager

Simplified PostgreSQL partitioning management

**Usage:**

```
postgresql-partition-manager
```

**Flags:**

| Flag | Shorthand | Default | Description |
|------|-----------|---------|-------------|
| --config | -c | "" | config file (default is $HOME/postgresql-partition-manager.yaml) |
| --connection-url | -u | "" | Database connection string |
| --debug | -d | false | Enable debug mode |
| --lock-timeout |  | 100 | Set lock_timeout (ms) |
| --log-format | -l | json | Log format (text or json) |
| --statement-timeout |  | 3000 | Set statement_timeout (ms) |

### postgresql-partition-manager run

Perform partition operations. Default partitions are not supported.

**Usage:**

```
postgresql-partition-manager run
```

**Inherited Flags:**

| Flag | Shorthand | Default | Description |
|------|-----------|---------|-------------|
| --config | -c | "" | config file (default is $HOME/postgresql-partition-manager.yaml) |
| --connection-url | -u | "" | Database connection string |
| --debug | -d | false | Enable debug mode |
| --lock-timeout |  | 100 | Set lock_timeout (ms) |
| --log-format | -l | json | Log format (text or json) |
| --statement-timeout |  | 3000 | Set statement_timeout (ms) |

#### postgresql-partition-manager run all

Perform partitions provisioning, cleanup, and check. Default partitions are not supported.

**Usage:**

```
postgresql-partition-manager run all
```

**Inherited Flags:**

| Flag | Shorthand | Default | Description |
|------|-----------|---------|-------------|
| --config | -c | "" | config file (default is $HOME/postgresql-partition-manager.yaml) |
| --connection-url | -u | "" | Database connection string |
| --debug | -d | false | Enable debug mode |
| --lock-timeout |  | 100 | Set lock_timeout (ms) |
| --log-format | -l | json | Log format (text or json) |
| --statement-timeout |  | 3000 | Set statement_timeout (ms) |

#### postgresql-partition-manager run check

Check existing partitions. Default partitions are not supported and may break checks.

**Usage:**

```
postgresql-partition-manager run check
```

**Inherited Flags:**

| Flag | Shorthand | Default | Description |
|------|-----------|---------|-------------|
| --config | -c | "" | config file (default is $HOME/postgresql-partition-manager.yaml) |
| --connection-url | -u | "" | Database connection string |
| --debug | -d | false | Enable debug mode |
| --lock-timeout |  | 100 | Set lock_timeout (ms) |
| --log-format | -l | json | Log format (text or json) |
| --statement-timeout |  | 3000 | Set statement_timeout (ms) |

#### postgresql-partition-manager run cleanup

Remove outdated partitions

**Usage:**

```
postgresql-partition-manager run cleanup
```

**Inherited Flags:**

| Flag | Shorthand | Default | Description |
|------|-----------|---------|-------------|
| --config | -c | "" | config file (default is $HOME/postgresql-partition-manager.yaml) |
| --connection-url | -u | "" | Database connection string |
| --debug | -d | false | Enable debug mode |
| --lock-timeout |  | 100 | Set lock_timeout (ms) |
| --log-format | -l | json | Log format (text or json) |
| --statement-timeout |  | 3000 | Set statement_timeout (ms) |

#### postgresql-partition-manager run provisioning

Create and attach new partitions. Default partitions are not supported.

**Usage:**

```
postgresql-partition-manager run provisioning
```

**Inherited Flags:**

| Flag | Shorthand | Default | Description |
|------|-----------|---------|-------------|
| --config | -c | "" | config file (default is $HOME/postgresql-partition-manager.yaml) |
| --connection-url | -u | "" | Database connection string |
| --debug | -d | false | Enable debug mode |
| --lock-timeout |  | 100 | Set lock_timeout (ms) |
| --log-format | -l | json | Log format (text or json) |
| --statement-timeout |  | 3000 | Set statement_timeout (ms) |

### postgresql-partition-manager validate

Check configuration file and exit with an error if configuration is invalid

**Usage:**

```
postgresql-partition-manager validate
```

**Inherited Flags:**

| Flag | Shorthand | Default | Description |
|------|-----------|---------|-------------|
| --config | -c | "" | config file (default is $HOME/postgresql-partition-manager.yaml) |
| --connection-url | -u | "" | Database connection string |
| --debug | -d | false | Enable debug mode |
| --lock-timeout |  | 100 | Set lock_timeout (ms) |
| --log-format | -l | json | Log format (text or json) |
| --statement-timeout |  | 3000 | Set statement_timeout (ms) |
