package ppm

import (
	"errors"
	"strconv"

	"github.com/qonto/postgresql-partition-manager/internal/infra/hook"
	partition_pkg "github.com/qonto/postgresql-partition-manager/internal/infra/partition"
)

// partitionOutcome describes the result of running a hook around a partition operation.
type partitionOutcome int

const (
	// outcomeCompleted means the hook succeeded and the operation may proceed.
	outcomeCompleted partitionOutcome = iota
	// outcomeSkipped means the remaining steps for this partition were skipped, but the run continues.
	outcomeSkipped
	// outcomeAbort means a hook requested that the entire process stop immediately.
	outcomeAbort
)

// isHookAbort reports whether the hook error signals an abort of the entire process
// (on_failure=abort). It logs the abort event so callers only need to act on the result.
func (p PPM) isHookAbort(hookErr error) bool {
	if errors.Is(hookErr, hook.ErrAbort) {
		p.logger.Error("Hook abort triggered, stopping entire run", "error", hookErr)

		return true
	}

	return false
}

// runHook executes a lifecycle hook and classifies the result against the on_failure policy.
// On failure it flips hookFailure and returns outcomeAbort (stop everything) or, for a
// non-abort failure, logs skipMsg and returns outcomeSkipped (skip remaining steps).
func (p PPM) runHook(exec func() error, hookFailure *bool, part partition_pkg.Partition, skipMsg string) partitionOutcome {
	hookErr := exec()
	if hookErr == nil {
		return outcomeCompleted
	}

	*hookFailure = true

	if p.isHookAbort(hookErr) {
		return outcomeAbort
	}

	p.logger.Warn(skipMsg, "partition", part.Name, "error", hookErr)

	return outcomeSkipped
}

// newHookOrchestrator builds the hook orchestrator for a partition set, resolving global vs
// partition-level hooks and wiring the runners/executor (or a no-op dry-run orchestrator).
func (p PPM) newHookOrchestrator(name string, config partition_pkg.Configuration, metrics *hook.MetricsCollector) *hook.Orchestrator {
	resolvedHooks := hook.Resolve(name, p.globalHooks, config.Hooks)
	if resolvedHooks != nil {
		resolvedHooks.ApplyDefaults()
	}

	if p.dryRun {
		return hook.NewDryRunOrchestrator(resolvedHooks, metrics, p.logger, p.connectionURL)
	}

	executor := hook.NewExecutor(hook.NewRegistryRunner(p.logger), p.logger)

	return hook.NewOrchestrator(resolvedHooks, executor, metrics, p.logger, p.connectionURL)
}

// buildPartitionContext assembles the template context exposed to hooks for a partition.
func (p PPM) buildPartitionContext(name string, config partition_pkg.Configuration, part partition_pkg.Partition) hook.PartitionContext {
	partCtx := hook.PartitionContext{
		Schema:        part.Schema,
		Table:         part.Name,
		ParentTable:   part.ParentTable,
		LowerBound:    part.LowerBound.Format("2006-01-02"),
		UpperBound:    part.UpperBound.Format("2006-01-02"),
		PartitionName: name,
		Retention:     strconv.Itoa(config.Retention),
		Interval:      string(config.Interval),
	}

	// Populate connection metadata if connection URL is available
	if p.connectionURL != "" {
		if creds, credErr := hook.ExtractCredentials(p.connectionURL); credErr == nil {
			partCtx.DatabaseName = creds["PGDATABASE"]
			partCtx.Hostname = creds["PGHOST"]
		}
	}

	return partCtx
}
