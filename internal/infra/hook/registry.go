package hook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

// ErrUnsupportedHookType is returned when a hook has a type with no registered handler.
var ErrUnsupportedHookType = errors.New("unsupported hook type")

// RenderedConfig is a type-specific hook configuration with all template variables already
// rendered. It knows how to describe itself for structured logging. Each hook type (shell,
// postgresql, ...) provides an implementation.
type RenderedConfig interface {
	// LogAttrs returns key/value pairs describing the resolved configuration for logging.
	LogAttrs() []any
}

// typeHandler bundles everything the framework needs to support a hook type. Registering a new
// handler is the only change required to add a new hook type (e.g. s3); the orchestrator,
// executor and dispatcher are type-agnostic.
type typeHandler struct {
	// validate checks the raw config map at configuration-load time.
	validate func(config map[string]interface{}) error

	// resolve renders template variables in the raw config map and returns a RenderedConfig.
	resolve func(config map[string]interface{}, partition PartitionContext) (RenderedConfig, error)

	// newRunner builds the Runner that executes this hook type.
	newRunner func(logger slog.Logger) Runner
}

// registry maps each supported hook type to its handler. To add a new hook type, implement a
// runner + RenderedConfig in its own file and register it here.
var registry = map[HookType]typeHandler{
	ShellType: {
		validate:  validateShellConfig,
		resolve:   resolveShellConfig,
		newRunner: func(logger slog.Logger) Runner { return NewShellRunner(logger) },
	},
	PostgreSQLType: {
		validate:  validatePostgreSQLConfig,
		resolve:   resolvePostgreSQLConfig,
		newRunner: func(logger slog.Logger) Runner { return NewPostgreSQLRunner(logger) },
	},
}

// RegistryRunner dispatches hook execution to the runner registered for the hook's type.
// It replaces hand-written type switches: support for a new type comes from the registry.
type RegistryRunner struct {
	runners map[HookType]Runner
}

// Compile-time check that RegistryRunner implements Runner.
var _ Runner = (*RegistryRunner)(nil)

// NewRegistryRunner builds a RegistryRunner with one runner instance per registered hook type.
func NewRegistryRunner(logger slog.Logger) *RegistryRunner {
	runners := make(map[HookType]Runner, len(registry))
	for hookType, handler := range registry {
		runners[hookType] = handler.newRunner(logger)
	}

	return &RegistryRunner{runners: runners}
}

// Run executes the hook using the runner registered for its type.
func (r *RegistryRunner) Run(ctx context.Context, hook *ResolvedHook) error {
	runner, ok := r.runners[hook.Type]
	if !ok {
		return fmt.Errorf("%w: %q for hook %q", ErrUnsupportedHookType, hook.Type, hook.Name)
	}

	if err := runner.Run(ctx, hook); err != nil {
		return fmt.Errorf("running %q hook %q: %w", hook.Type, hook.Name, err)
	}

	return nil
}
