// Package postgresql provides the database operations layer for the
// table partition conversion feature.
package postgresql

import "time"

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

// ColumnDef represents a column definition from the source table schema.
type ColumnDef struct {
	Name         string
	DataType     string
	IsNullable   bool
	DefaultValue *string
	IsGenerated  bool
}

// IndexDef represents an index definition from the source table.
type IndexDef struct {
	Name       string
	Columns    []string
	IsUnique   bool
	IsPrimary  bool
	Predicate  *string // For partial indexes
	Expression *string // For expression indexes
	Method     string  // btree, hash, gin, gist, etc.
}

// ForeignKeyDef represents a foreign key constraint definition.
type ForeignKeyDef struct {
	Name              string
	Columns           []string
	ReferencedSchema  string
	ReferencedTable   string
	ReferencedColumns []string
	OnDelete          string
	OnUpdate          string
}

// CheckConstraintDef represents a CHECK constraint definition.
type CheckConstraintDef struct {
	Name       string
	Expression string
}

// VerifyResult contains the results of a convergence verification check.
type VerifyResult struct {
	SourceRowCount  int64
	TargetRowCount  int64
	RowDifference   int64
	ReplayLag       int64
	ReadyForCutover bool
}
