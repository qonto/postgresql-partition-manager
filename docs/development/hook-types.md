# Developing a Hook Type

This page is for contributors who want to add a new **hook type** (a "runner") to
PostgreSQL Partition Manager — for example an `s3` type that archives a partition
to object storage.

If you only want to *use* the existing hook types, see [Shell](../hooks/shell.md) and
[PostgreSQL](../hooks/postgresql.md) instead.

The hook engine lives in the `internal/infra/hook` package.

## Architecture

A hook type is described by three pieces of behavior, bundled in a `typeHandler`
(in `registry.go`):

| Field       | Responsibility                                              |
| ----------- | ----------------------------------------------------------- |
| `validate`  | Check the raw `config` map at configuration-load time.      |
| `resolve`   | Render template variables and build a typed config.         |
| `newRunner` | Build the runner that executes the hook.                    |

These are wired together by the `registry` map. Everything else is
type-agnostic and does **not** change when you add a type:

- **Orchestrator** — resolves, logs, executes, applies the `on_failure` policy, records metrics.
- **Executor** — adds timeout and retry/backoff around any runner.
- **RegistryRunner** — dispatches to the right runner based on the hook type.

The lifecycle of a single hook:

```text
HookEntry (raw config map)
    │  validate   ← at config load
    │  resolve    ← templates applied → typed config
    ▼
ResolvedHook { Type, Config, ConnectionURL, PartitionContext, ... }
    │  Executor (timeout + retry)
    │  RegistryRunner → runner for Type
    ▼
Runner.Run(ctx, hook)   ← your runner does the work
```

## Interfaces you implement

```go
// RenderedConfig is your config after template variables are substituted.
// It only has to describe itself for structured logging.
type RenderedConfig interface {
	LogAttrs() []any // key/value pairs, e.g. []any{"bucket", c.Bucket}
}

// Runner executes the hook. The context carries the timeout deadline.
type Runner interface {
	Run(ctx context.Context, hook *ResolvedHook) error
}
```

At execution time your runner receives a `*ResolvedHook`. Type-assert
`hook.Config` to your concrete config type. `hook.ConnectionURL` carries the
database connection details if you need them.

!!! warning
    `LogAttrs` output is emitted at debug level and in `--dry-run`. Only return
    fields that are safe to log — never secrets.

## Step by step: an `s3` runner

Everything below goes in a single new file `s3_runner.go`, plus **one line** in
the `registry` map and **one** type constant.

### 1. Add the type constant

In `config.go`:

```go
const (
	ShellType      HookType = "shell"
	PostgreSQLType HookType = "postgresql"
	S3Type         HookType = "s3" // new
)
```

The type-validity check in `HookEntry.Validate` is registry-driven, so
registering the handler (step 5) is what makes `type: s3` accepted — there is no
switch to update.

### 2. Define the rendered config and `LogAttrs`

```go
type S3Config struct {
	Bucket string `mapstructure:"bucket"`
	Key    string `mapstructure:"key"`
}

var _ RenderedConfig = (*S3Config)(nil)

func (c *S3Config) LogAttrs() []any {
	return []any{"bucket", c.Bucket, "key", c.Key}
}
```

### 3. Validate the raw config

Runs at config-load time, before any partition work. Use static, wrapped error
variables (mirror the existing `Err*` vars in `config.go`):

```go
var (
	ErrS3ConfigRequired = errors.New("config section is required for s3 hooks")
	ErrS3BucketRequired = errors.New("'bucket' is required in config for s3 hooks")
)

func validateS3Config(config map[string]interface{}) error {
	if config == nil {
		return ErrS3ConfigRequired
	}
	if _, ok := config["bucket"]; !ok {
		return ErrS3BucketRequired
	}
	return nil
}
```

### 4. Resolve templates

Render every user-supplied string field with `Render(value, partition)` so
[template variables](../hooks/index.md#template-variables) such as `{{.Schema}}` and
`{{.Table}}` work:

```go
func resolveS3Config(config map[string]interface{}, partition PartitionContext) (RenderedConfig, error) {
	cfg := &S3Config{}

	if v, ok := config["bucket"]; ok {
		rendered, err := Render(fmt.Sprintf("%v", v), partition)
		if err != nil {
			return nil, fmt.Errorf("rendering bucket: %w", err)
		}
		cfg.Bucket = rendered
	}

	if v, ok := config["key"]; ok {
		rendered, err := Render(fmt.Sprintf("%v", v), partition)
		if err != nil {
			return nil, fmt.Errorf("rendering key: %w", err)
		}
		cfg.Key = rendered
	}

	return cfg, nil
}
```

### 5. Implement the runner

```go
var _ Runner = (*S3Runner)(nil)

type S3Runner struct {
	logger slog.Logger
}

func NewS3Runner(logger slog.Logger) *S3Runner {
	return &S3Runner{logger: logger}
}

func (r *S3Runner) Run(ctx context.Context, hook *ResolvedHook) error {
	cfg, ok := hook.Config.(*S3Config)
	if !ok {
		return fmt.Errorf("s3 configuration is nil for hook %q", hook.Name)
	}

	r.logger.Debug("Executing s3 hook", "hook", hook.Name, "bucket", cfg.Bucket, "key", cfg.Key)

	// ... perform the upload, honoring ctx for the timeout deadline ...

	return nil
}
```

!!! tip
    Honor `ctx` — it carries the per-hook timeout. Retry and backoff are handled
    for you by the executor based on the hook's `retry` config, so do not add
    your own retry loop.

### 6. Register the handler

This is the only edit outside your new file, in `registry.go`:

```go
var registry = map[HookType]typeHandler{
	ShellType:      { /* ... */ },
	PostgreSQLType: { /* ... */ },
	S3Type: {
		validate:  validateS3Config,
		resolve:   resolveS3Config,
		newRunner: func(logger slog.Logger) Runner { return NewS3Runner(logger) },
	},
}
```

That is all. The orchestrator, executor, dispatcher, logging, dry-run, metrics,
and config validation now support `type: s3`.

## Checklist

- [ ] `HookType` constant added in `config.go`
- [ ] Config struct implements `RenderedConfig` (`LogAttrs` + compile-time `var _`)
- [ ] `validateXxxConfig` with static, wrapped error variables
- [ ] `resolveXxxConfig` renders every templated field via `Render`
- [ ] Runner implements `Runner`, type-asserts `hook.Config`, and honors `ctx`
- [ ] Handler registered in the `registry` map
- [ ] Unit tests for the runner, resolve, and validate functions
- [ ] User documentation added under `docs/hooks/` and linked in `mkdocs.yml`
- [ ] `LogAttrs` exposes no secrets

## Testing conventions

Mirror the existing tests in the `internal/infra/hook` package:

- **Runner** — table-driven success / failure / nil-config cases (see
  `shell_runner_test.go`). For runners with external dependencies, inject a seam
  (like `PostgreSQLRunner`'s `ConnectorFunc`) so tests don't hit the network.
- **Validation** — assert the specific `Err*` sentinel with `errors.Is` (see
  `config_test.go`).
- **Resolution** — assert template variables are substituted, and that an
  unknown variable surfaces an error (templates use `missingkey=error`).

Run the suite before opening a pull request:

```bash
make test
make lint
```
