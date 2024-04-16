package postgresql_test

import (
	"testing"

	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
	"gotest.tools/assert"
)

func TestColumnAttributes(t *testing.T) {
	testCases := []struct {
		name         string
		column       postgresql.Column
		expectedName string
	}{
		{
			name: "Public schema",
			column: postgresql.Column{
				Schema: "public",
				Table:  "my_table",
				Name:   "my_column",
			},
			expectedName: "public.my_table.my_column",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.column.String(), tc.expectedName, "Column name don't match")
		})
	}
}
