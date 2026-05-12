// Package postgresql provides the database operations layer for the
// table partition conversion feature.
package postgresql

import (
	"time"

	infra "github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
)

// Type aliases for schema introspection types defined in the parent postgresql package.
// This allows the convert package to use these types without breaking existing references.
type ColumnDef = infra.ColumnDef
type IndexDef = infra.IndexDef
type ForeignKeyDef = infra.ForeignKeyDef
type CheckConstraintDef = infra.CheckConstraintDef

// CDCEvent represents a single change data capture event from the CDC queue.
type CDCEvent struct {
	SeqID     int64
	Operation string // "INSERT", "UPDATE", "DELETE"
	PKValues  []string
	CreatedAt time.Time
}

// MigrationState represents the persistent state of a table conversion migration.
// The Phase field is a string that corresponds to the Phase constants defined
// in pkg/convert (PhaseSetup, PhaseBackfill, PhaseReplay, etc.).
type MigrationState struct {
	Schema             string
	Table              string
	Phase              string // Corresponds to convert.Phase constants
	LastBackfillPK     []string
	LastReplaySeq      int64
	PhaseStartedAt     time.Time
	UpdatedAt          time.Time
	DroppedForeignKeys []ForeignKeyDef // FKs dropped pre-cutover, stored for recreation/rollback
}

// VerifyResult contains the results of a convergence verification check.
type VerifyResult struct {
	SourceRowCount  int64
	TargetRowCount  int64
	RowDifference   int64
	ReplayLag       int64
	ReadyForCutover bool
}
