package hook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// ErrAbort is returned when a hook with on_failure=abort fails, signaling
// that the entire cleanup process should stop immediately.
var ErrAbort = errors.New("hook abort: stopping entire cleanup process")

// Orchestrator coordinates hook execution around cleanup operations.
// It executes hooks sequentially, handles failure policies, and tracks metrics.
type Orchestrator struct {
	hooks      *HooksConfig
	executor   *Executor
	metrics    *MetricsCollector
	logger     slog.Logger
	connURL    string
	hasFailure bool
	dryRun     bool
}

// NewOrchestrator creates a new Orchestrator with the given configuration.
// If hooks is nil, all Execute* methods become no-ops.
func NewOrchestrator(hooks *HooksConfig, executor *Executor, metrics *MetricsCollector, logger slog.Logger, connectionURL string) *Orchestrator {
	return &Orchestrator{
		hooks:    hooks,
		executor: executor,
		metrics:  metrics,
		logger:   logger,
		connURL:  connectionURL,
	}
}

// NewDryRunOrchestrator creates an Orchestrator that resolves templates and logs
// resolved hook configurations without actually executing hooks.
func NewDryRunOrchestrator(hooks *HooksConfig, metrics *MetricsCollector, logger slog.Logger, connectionURL string) *Orchestrator {
	return &Orchestrator{
		hooks:   hooks,
		metrics: metrics,
		logger:  logger,
		connURL: connectionURL,
		dryRun:  true,
	}
}

// ExecuteBeforeDetach runs before-detach hooks for a partition.
// Returns an error if hooks fail and the detach operation should be cancelled (default behavior).
// Returns ErrAbort if a hook with on_failure=abort fails.
// Returns nil if on_failure=continue is set on the failing hook.
func (o *Orchestrator) ExecuteBeforeDetach(ctx context.Context, partition PartitionContext) error {
	return o.Execute(ctx, BeforeDetach, partition)
}

// ExecuteAfterDetach runs after-detach hooks for a partition.
// Returns an error if hooks fail (caller decides whether to skip drop).
// Returns ErrAbort if a hook with on_failure=abort fails.
func (o *Orchestrator) ExecuteAfterDetach(ctx context.Context, partition PartitionContext) error {
	return o.Execute(ctx, AfterDetach, partition)
}

// ExecuteBeforeDrop runs before-drop hooks for a partition.
// Returns an error if hooks fail and the drop operation should be cancelled (default behavior).
// Returns ErrAbort if a hook with on_failure=abort fails.
// Returns nil if on_failure=continue is set on the failing hook.
func (o *Orchestrator) ExecuteBeforeDrop(ctx context.Context, partition PartitionContext) error {
	return o.Execute(ctx, BeforeDrop, partition)
}

// ExecuteAfterDrop runs after-drop hooks for a partition.
// Returns an error if hooks fail (informational, operation already done).
// Returns ErrAbort if a hook with on_failure=abort fails.
func (o *Orchestrator) ExecuteAfterDrop(ctx context.Context, partition PartitionContext) error {
	return o.Execute(ctx, AfterDrop, partition)
}

// Execute runs the hooks configured for the given lifecycle event against a partition.
// It is a no-op (returns nil) when no hooks are configured.
func (o *Orchestrator) Execute(ctx context.Context, event LifecycleEvent, partition PartitionContext) error {
	if o.hooks == nil {
		return nil
	}

	return o.executeHooks(ctx, partition, o.hooks.hooksFor(event), event)
}

// HasFailures returns true if any hook failed during the cleanup process.
func (o *Orchestrator) HasFailures() bool {
	return o.hasFailure
}

// Summary returns the aggregate execution metrics.
func (o *Orchestrator) Summary() ExecutionSummary {
	return o.metrics.Summary()
}

// executeHooks runs a list of hooks sequentially for a given lifecycle event.
// It skips disabled hooks, short-circuits on failure, and applies on_failure policies.
// In dry-run mode, it resolves templates and logs the resolved configuration without executing.
func (o *Orchestrator) executeHooks(ctx context.Context, partition PartitionContext, hooks []HookEntry, event LifecycleEvent) error {
	for i := range hooks {
		entry := hooks[i]

		// Skip disabled hooks without error (Requirement 2.7)
		if entry.Enabled != nil && !*entry.Enabled {
			o.logger.Debug("Skipping disabled hook",
				"hook", entry.Name,
				"lifecycle_event", string(event),
				"partition", partition.PartitionName,
			)

			continue
		}

		// Resolve template variables and build the ResolvedHook
		resolved, err := o.resolveHook(entry, event, partition)
		if err != nil {
			o.hasFailure = true
			o.recordMetric(entry, event, partition, 0, "failure", 0)

			// Template variable errors are reported in both normal and dry-run mode (Requirement 17.7)
			return o.handleFailure(entry, event, fmt.Errorf("template rendering failed for hook %q: %w", entry.Name, err))
		}

		// In dry-run mode: log the resolved hook configuration and skip execution (Requirements 17.2, 17.3, 17.5)
		if o.dryRun {
			o.logHookConfig(entry, event, partition, resolved, true)

			continue
		}

		// Log resolved hook configuration at debug level (Requirement 16.4)
		o.logHookConfig(entry, event, partition, resolved, false)

		// Log hook start (Requirement 16.1)
		o.logger.Info("Executing hook",
			"hook", entry.Name,
			"type", string(entry.Type),
			"lifecycle_event", string(event),
			"partition", partition.PartitionName,
		)

		// Execute the hook with retry/timeout via the Executor
		start := time.Now()
		execErr := o.executor.Execute(ctx, resolved, entry)
		duration := time.Since(start)

		if execErr != nil {
			o.hasFailure = true
			o.recordMetric(entry, event, partition, duration, "failure", entry.Retry.Attempts)

			o.logger.Error("Hook execution failed",
				"hook", entry.Name,
				"type", string(entry.Type),
				"lifecycle_event", string(event),
				"partition", partition.PartitionName,
				"error", execErr.Error(),
				"duration_ms", duration.Milliseconds(),
			)

			// Short-circuit: remaining hooks in this event are skipped (Requirements 6.2, 6.6)
			return o.handleFailure(entry, event, execErr)
		}

		o.recordMetric(entry, event, partition, duration, "success", 0)

		o.logger.Info("Hook execution succeeded",
			"hook", entry.Name,
			"type", string(entry.Type),
			"lifecycle_event", string(event),
			"partition", partition.PartitionName,
			"duration_ms", duration.Milliseconds(),
		)
	}

	return nil
}

// handleFailure applies the on_failure policy for a failed hook.
// - abort: returns ErrAbort to stop the entire cleanup process
// - continue: returns nil so the operation proceeds despite the failure
// - default (unset): before-hooks return error (cancel operation), after-hooks return error (informational)
func (o *Orchestrator) handleFailure(entry HookEntry, event LifecycleEvent, execErr error) error {
	switch entry.OnFailure {
	case OnFailureAbort:
		// Requirement 6.8: stop entire cleanup process immediately
		return fmt.Errorf("%w: hook %q failed: %w", ErrAbort, entry.Name, execErr)
	case OnFailureContinue:
		// Requirement 6.9: proceed with the operation despite hook failure
		o.logger.Warn("Hook failed but on_failure=continue, proceeding",
			"hook", entry.Name,
			"lifecycle_event", string(event),
			"error", execErr.Error(),
		)

		return nil
	default:
		// Default behavior depends on lifecycle position
		if isBeforeHook(event) {
			// Before-hook failure cancels the associated operation (Requirements 6.1, 9.2, 11.2)
			return fmt.Errorf("hook %q failed, cancelling %s operation: %w", entry.Name, string(event), execErr)
		}

		// After-hook failure: log error, operation already done (Requirement 6.4)
		// Still return error so caller can decide (e.g., skip drop after after-detach failure)
		return fmt.Errorf("hook %q failed during %s: %w", entry.Name, string(event), execErr)
	}
}

// resolveHook renders template variables and builds a ResolvedHook ready for execution.
func (o *Orchestrator) resolveHook(entry HookEntry, event LifecycleEvent, partition PartitionContext) (*ResolvedHook, error) {
	resolved := &ResolvedHook{
		Name:             entry.Name,
		Type:             entry.Type,
		LifecycleEvent:   event,
		PartitionContext: partition,
		ConnectionURL:    o.connURL,
	}

	handler, ok := registry[entry.Type]
	if !ok {
		return nil, fmt.Errorf("%w: %q for hook %q", ErrUnsupportedHookType, entry.Type, entry.Name)
	}

	rendered, err := handler.resolve(entry.Config, partition)
	if err != nil {
		return nil, err
	}

	resolved.Config = rendered

	return resolved, nil
}

// recordMetric records a hook execution metric via the MetricsCollector.
func (o *Orchestrator) recordMetric(entry HookEntry, event LifecycleEvent, partition PartitionContext, duration time.Duration, outcome string, retryAttempts int) {
	o.metrics.Record(HookMetric{
		HookName:       entry.Name,
		HookType:       entry.Type,
		LifecycleEvent: event,
		PartitionName:  partition.PartitionName,
		Duration:       duration,
		Outcome:        outcome,
		RetryAttempts:  retryAttempts,
	})
}

// isBeforeHook returns true if the lifecycle event is a before-* event.
func isBeforeHook(event LifecycleEvent) bool {
	return event == BeforeDetach || event == BeforeDrop
}

// logHookConfig logs the fully resolved hook configuration (with template variables substituted).
// When dryRun is true, it logs at info level to announce what would be executed (Requirements 17.2, 17.5).
// Otherwise it logs at debug level so operators can inspect the resolved configuration (Requirement 16.4).
func (o *Orchestrator) logHookConfig(entry HookEntry, event LifecycleEvent, partition PartitionContext, resolved *ResolvedHook, dryRun bool) {
	logArgs := []any{
		"hook", entry.Name,
		"type", string(entry.Type),
		"lifecycle_event", string(event),
		"partition", partition.PartitionName,
	}

	if resolved.Config != nil {
		logArgs = append(logArgs, resolved.Config.LogAttrs()...)
	}

	if entry.Timeout > 0 {
		logArgs = append(logArgs, "timeout", entry.Timeout.String())
	}

	if entry.OnFailure != "" {
		logArgs = append(logArgs, "on_failure", string(entry.OnFailure))
	}

	if entry.Retry.Attempts > 0 {
		logArgs = append(logArgs,
			"retry_attempts", entry.Retry.Attempts,
			"retry_backoff", string(entry.Retry.Backoff),
		)
	}

	if dryRun {
		o.logger.Info("[DRY-RUN] Would execute hook", append(logArgs, "dry_run", true)...)

		return
	}

	o.logger.Debug("Resolved hook configuration", logArgs...)
}
