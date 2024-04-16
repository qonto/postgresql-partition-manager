package postgresql_test

import (
	"testing"

	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
	"github.com/stretchr/testify/assert"
)

func TestTableAttributes(t *testing.T) {
	testCases := []struct {
		name         string
		table        postgresql.Table
		expectedName string
	}{
		{
			name: "Public schema",
			table: postgresql.Table{
				Schema: "public",
				Name:   "my_table",
			},
			expectedName: "public.my_table",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.table.QualifiedName(), tc.expectedName, "Table name name don't match")
			assert.Equal(t, tc.table.String(), tc.expectedName, "Table name name don't match")
		})
	}
}
