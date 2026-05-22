// Package convert provides the core conversion logic for migrating
// non-partitioned PostgreSQL tables to range-partitioned tables.
package convert

// Phase represents a migration phase in the conversion lifecycle.
type Phase string

const (
	PhaseSetup            Phase = "setup"
	PhaseBackfill         Phase = "backfill"
	PhaseReplay           Phase = "replay"
	PhaseVerify           Phase = "verify"
	PhaseCutover          Phase = "cutover"
	PhaseCleanup          Phase = "cleanup"
	PhaseRollbackComplete Phase = "rollback_complete"
)

// AllowedTransitions defines valid phase transitions.
// Note: PhaseVerify is informational and does not modify state going forward,
// but we define transitions from it to handle databases where "verify" was
// previously persisted as the current phase.
var AllowedTransitions = map[Phase][]Phase{
	PhaseSetup:            {PhaseBackfill},
	PhaseBackfill:         {PhaseReplay, PhaseBackfill, PhaseCutover},
	PhaseReplay:           {PhaseCutover, PhaseReplay},
	PhaseVerify:           {PhaseCutover, PhaseReplay, PhaseBackfill},
	PhaseCutover:          {PhaseCleanup, PhaseRollbackComplete},
	PhaseRollbackComplete: {PhaseSetup},
}
