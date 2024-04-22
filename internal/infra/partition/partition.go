// Package partition provides methods for PostgreSQL partition configuration
package partition

import (
	"fmt"
	"time"
)

type Partition struct {
	ParentTable string
	Schema      string
	Name        string
	LowerBound  time.Time
	UpperBound  time.Time
}

func (p Partition) String() string {
	return p.QualifiedName()
}

// QualifiedName returns the fully qualified name of the partition (format: <schema>.<table>)
// This is recommended to avoid schema conflicts when querying PostgreSQL catalog tables
func (p Partition) QualifiedName() string {
	return fmt.Sprintf("%s.%s", p.Schema, p.Name)
}
