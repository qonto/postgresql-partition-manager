package convert

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/convert/postgresql"
)

// StateMachine manages migration state transitions and persistence.
// It validates that phase transitions follow the AllowedTransitions map
// and persists state changes via the ConvertDBClient.
type StateMachine struct {
	db     ConvertDBClient
	logger slog.Logger
}

// NewStateMachine creates a new StateMachine with the given database client and logger.
func NewStateMachine(logger slog.Logger, db ConvertDBClient) *StateMachine {
	return &StateMachine{
		db:     db,
		logger: logger,
	}
}

// ValidateTransition checks whether a transition from current to requested phase is allowed.
// Returns nil if the transition is valid, or an error identifying the current phase,
// the requested phase, and the list of permitted next phases.
func (sm *StateMachine) ValidateTransition(current, requested Phase) error {
	allowed, exists := AllowedTransitions[current]
	if !exists {
		return fmt.Errorf("no transitions defined from phase %q; requested %q", current, requested)
	}

	for _, p := range allowed {
		if p == requested {
			return nil
		}
	}

	return fmt.Errorf(
		"transition from %q to %q is not allowed; permitted transitions from %q: %v",
		current, requested, current, allowed,
	)
}

// GetState retrieves the current migration state for the given schema and table.
// If no state exists, it initializes a new state with PhaseSetup and persists it.
// It ensures the metadata table exists before attempting to read state.
func (sm *StateMachine) GetState(schema, table string) (*postgresql.MigrationState, error) {
	if err := sm.db.EnsureMetadataTable(); err != nil {
		return nil, fmt.Errorf("failed to ensure metadata table: %w", err)
	}

	state, err := sm.db.GetMigrationState(schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get migration state for %s.%s: %w", schema, table, err)
	}

	if state == nil {
		now := time.Now()
		state = &postgresql.MigrationState{
			Schema:         schema,
			Table:          table,
			Phase:          string(PhaseSetup),
			PhaseStartedAt: now,
			UpdatedAt:      now,
		}

		if err := sm.db.UpdateMigrationState(schema, table, state); err != nil {
			return nil, fmt.Errorf("failed to initialize migration state for %s.%s: %w", schema, table, err)
		}

		sm.logger.Info("initialized migration state", "schema", schema, "table", table, "phase", PhaseSetup)
	}

	return state, nil
}

// TransitionTo validates and persists a phase transition for the given schema and table.
// It reads the current state, validates the transition, updates the phase, and persists the change.
func (sm *StateMachine) TransitionTo(schema, table string, phase Phase) error {
	state, err := sm.GetState(schema, table)
	if err != nil {
		return err
	}

	currentPhase := Phase(state.Phase)

	if err := sm.ValidateTransition(currentPhase, phase); err != nil {
		sm.logger.Error("invalid phase transition",
			"schema", schema,
			"table", table,
			"current_phase", currentPhase,
			"requested_phase", phase,
			"permitted_transitions", AllowedTransitions[currentPhase],
		)

		return err
	}

	now := time.Now()
	state.Phase = string(phase)
	state.PhaseStartedAt = now
	state.UpdatedAt = now

	if err := sm.db.UpdateMigrationState(schema, table, state); err != nil {
		return fmt.Errorf("failed to persist phase transition to %q for %s.%s: %w", phase, schema, table, err)
	}

	sm.logger.Info("phase transition completed",
		"schema", schema,
		"table", table,
		"from_phase", currentPhase,
		"to_phase", phase,
	)

	return nil
}
