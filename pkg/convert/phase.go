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
var AllowedTransitions = map[Phase][]Phase{
	PhaseSetup:            {PhaseBackfill},
	PhaseBackfill:         {PhaseReplay, PhaseBackfill},
	PhaseReplay:           {PhaseVerify, PhaseReplay},
	PhaseVerify:           {PhaseCutover, PhaseReplay, PhaseVerify},
	PhaseCutover:          {PhaseCleanup, PhaseRollbackComplete},
	PhaseRollbackComplete: {PhaseSetup},
}
