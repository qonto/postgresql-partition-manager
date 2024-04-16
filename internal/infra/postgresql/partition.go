package postgresql

import "fmt"

type Partition struct {
	ParentTable string
	Schema      string
	Name        string
	LowerBound  interface{}
	UpperBound  interface{}
}

func (p Partition) String() string {
	return p.QualifiedName()
}

// QualifiedName returns the fully qualified name of the partition (format: <schema>.<table>)
// This is recommended to avoid schema conflicts when querying PostgreSQL catalog tables
func (p Partition) QualifiedName() string {
	return fmt.Sprintf("%s.%s", p.Schema, p.Name)
}

func (p Partition) ToTable() Table {
	return Table{
		Schema: p.Schema,
		Name:   p.Name,
	}
}

func (p Partition) GetParentTable() Table {
	return Table{
		Schema: p.Schema,
		Name:   p.ParentTable,
	}
}
