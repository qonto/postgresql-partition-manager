package postgresql

import "fmt"

type Table struct {
	Schema string
	Name   string
}

func (t Table) String() string {
	return t.QualifiedName()
}

// QualifiedName returns the fully qualified name of the table (format: <schema>.<table>)
// This is recommended to avoid schema conflicts when querying PostgreSQL catalog tables
func (t Table) QualifiedName() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.Name)
}
