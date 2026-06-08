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

Perform partition operations

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

Perform partitions provisioning, cleanup, and check

**Usage:**

```
postgresql-partition-manager run all [flags]
```

**Flags:**

| Flag | Shorthand | Default | Description |
|------|-----------|---------|-------------|
| --dry-run |  | false | Preview which hooks would be executed without actually running them |

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

Check existing partitions

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
postgresql-partition-manager run cleanup [flags]
```

**Flags:**

| Flag | Shorthand | Default | Description |
|------|-----------|---------|-------------|
| --dry-run |  | false | Preview which hooks would be executed without actually running them |

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

Create and attach new partitions

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

