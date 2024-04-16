package postgresql_test

import (
	"testing"

	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
	"gotest.tools/assert"
)

func TestPartitionSettings(t *testing.T) {
	var UnsupportedColumnType postgresql.ColumnType = "unsupported"

	testCases := []struct {
		name                 string
		partition            postgresql.PartitionSettings
		supportedKeyDataType bool
		supportedStrategy    bool
	}{
		{
			name: "Public",
			partition: postgresql.PartitionSettings{
				Strategy: postgresql.RangePartitionStrategy,
				KeyType:  postgresql.DateColumnType,
			},
			supportedStrategy:    true,
			supportedKeyDataType: true,
		},
		{
			name: "Unsupported list strategy",
			partition: postgresql.PartitionSettings{
				Strategy: postgresql.ListPartitionStrategy,
				KeyType:  postgresql.DateColumnType,
			},
			supportedKeyDataType: true,
			supportedStrategy:    false,
		},
		{
			name: "Unsupported hash strategy",
			partition: postgresql.PartitionSettings{
				Strategy: postgresql.HashPartitionStrategy,
				KeyType:  postgresql.DateColumnType,
			},
			supportedKeyDataType: true,
			supportedStrategy:    false,
		},
		{
			name: "Unsupported column type",
			partition: postgresql.PartitionSettings{
				Strategy: postgresql.RangePartitionStrategy,
				KeyType:  UnsupportedColumnType,
			},
			supportedKeyDataType: false,
			supportedStrategy:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.partition.SupportedStrategy(), tc.supportedStrategy, "Supported strategy mismatch")
			assert.Equal(t, tc.partition.SupportedKeyDataType(), tc.supportedKeyDataType, "Supported key data type mismatch")
		})
	}
}
