package hook

import "context"

// ResolvedHook contains a hook entry with all template variables already rendered,
// ready for execution by a Runner.
type ResolvedHook struct {
	// Name is the hook name for identification in logs.
	Name string

	// Type identifies which runner implementation to use.
	Type HookType

	// LifecycleEvent indicates when this hook executes (before-detach, after-detach, etc.).
	LifecycleEvent LifecycleEvent

	// Config holds the rendered, type-specific configuration. Its concrete type matches the
	// hook Type (e.g. *ShellConfig for ShellType). Runners type-assert it to their own type.
	Config RenderedConfig

	// PartitionContext holds the partition metadata used for template rendering.
	PartitionContext PartitionContext

	// ConnectionURL is the PostgreSQL connection URL used by the PostgreSQL runner
	// and for credential propagation to shell hooks.
	ConnectionURL string
}

// Runner executes a specific hook type.
type Runner interface {
	// Run executes the hook and returns an error if it fails.
	// The context carries the timeout deadline.
	Run(ctx context.Context, hook *ResolvedHook) error
}
