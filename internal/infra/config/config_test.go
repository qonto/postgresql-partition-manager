package config

import (
	"testing"

	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validConfig() *Config {
	return &Config{
		StatementTimeout: 3000,
		LockTimeout:      100,
		Partitions: map[string]partition.Configuration{
			"my-events": {
				Schema:         "public",
				Table:          "events",
				PartitionKey:   "created_at",
				Interval:       partition.Daily,
				Retention:      3,
				PreProvisioned: 3,
				CleanupPolicy:  partition.Drop,
			},
		},
	}
}

func TestCheck_WithoutConvertKey(t *testing.T) {
	cfg := validConfig()

	err := cfg.Check()
	assert.NoError(t, err, "config without convert key should pass validation")
}

func TestCheck_WithEmptyConvert(t *testing.T) {
	cfg := validConfig()
	cfg.Partitions["my-events"] = partition.Configuration{
		Schema:         "public",
		Table:          "events",
		PartitionKey:   "created_at",
		Interval:       partition.Daily,
		Retention:      3,
		PreProvisioned: 3,
		CleanupPolicy:  partition.Drop,
		Convert:        &partition.ConvertSettings{},
	}

	err := cfg.Check()
	assert.NoError(t, err, "config with empty convert (all zeros) should pass validation because of omitempty on fields")
}

func TestCheck_WithValidConvertValues(t *testing.T) {
	cfg := validConfig()
	cfg.Partitions["my-events"] = partition.Configuration{
		Schema:         "public",
		Table:          "events",
		PartitionKey:   "created_at",
		Interval:       partition.Daily,
		Retention:      3,
		PreProvisioned: 3,
		CleanupPolicy:  partition.Drop,
		Convert: &partition.ConvertSettings{
			BackfillBatchSize: 5000,
			ReplayBatchSize:   2000,
			LockTimeout:       10,
			StatementTimeout:  60,
		},
	}

	err := cfg.Check()
	assert.NoError(t, err, "config with valid convert values should pass validation")
}

func TestCheck_WithInvalidConvertValues(t *testing.T) {
	tests := []struct {
		name    string
		convert *partition.ConvertSettings
	}{
		{
			name: "negative backfillBatchSize",
			convert: &partition.ConvertSettings{
				BackfillBatchSize: -1,
			},
		},
		{
			name: "backfillBatchSize exceeds max",
			convert: &partition.ConvertSettings{
				BackfillBatchSize: 1000001,
			},
		},
		{
			name: "negative replayBatchSize",
			convert: &partition.ConvertSettings{
				ReplayBatchSize: -1,
			},
		},
		{
			name: "lockTimeout exceeds max",
			convert: &partition.ConvertSettings{
				LockTimeout: 61,
			},
		},
		{
			name: "statementTimeout below min",
			convert: &partition.ConvertSettings{
				StatementTimeout: 4,
			},
		},
		{
			name: "statementTimeout exceeds max",
			convert: &partition.ConvertSettings{
				StatementTimeout: 121,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.Partitions["my-events"] = partition.Configuration{
				Schema:         "public",
				Table:          "events",
				PartitionKey:   "created_at",
				Interval:       partition.Daily,
				Retention:      3,
				PreProvisioned: 3,
				CleanupPolicy:  partition.Drop,
				Convert:        tt.convert,
			}

			err := cfg.Check()
			assert.Error(t, err, "config with invalid convert values should fail validation")
		})
	}
}

func TestGetConvertConfig_Found(t *testing.T) {
	cfg := &Config{
		Partitions: map[string]partition.Configuration{
			"my-events": {
				Schema:         "public",
				Table:          "events",
				PartitionKey:   "created_at",
				Interval:       partition.Daily,
				Retention:      3,
				PreProvisioned: 3,
				CleanupPolicy:  partition.Drop,
				Convert: &partition.ConvertSettings{
					BackfillBatchSize: 5000,
					ReplayBatchSize:   2000,
					LockTimeout:       10,
					StatementTimeout:  60,
				},
			},
		},
	}

	result, err := cfg.GetConvertConfig("my-events")
	require.NoError(t, err)
	assert.Equal(t, "public", result.Schema)
	assert.Equal(t, "events", result.Table)
	assert.NotNil(t, result.Convert)
	assert.Equal(t, 5000, result.Convert.BackfillBatchSize)
	assert.Equal(t, 2000, result.Convert.ReplayBatchSize)
	assert.Equal(t, 10, result.Convert.LockTimeout)
	assert.Equal(t, 60, result.Convert.StatementTimeout)
}

func TestGetConvertConfig_NotFound(t *testing.T) {
	cfg := &Config{
		Partitions: map[string]partition.Configuration{},
	}

	_, err := cfg.GetConvertConfig("nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `partition "nonexistent" not found in configuration`)
}

func TestGetConvertConfig_NilConvert(t *testing.T) {
	cfg := &Config{
		Partitions: map[string]partition.Configuration{
			"my-events": {
				Schema:         "public",
				Table:          "events",
				PartitionKey:   "created_at",
				Interval:       partition.Daily,
				Retention:      3,
				PreProvisioned: 3,
				CleanupPolicy:  partition.Drop,
				Convert:        nil,
			},
		},
	}

	result, err := cfg.GetConvertConfig("my-events")
	require.NoError(t, err)
	assert.NotNil(t, result.Convert)
	assert.Equal(t, 10000, result.Convert.BackfillBatchSize)
	assert.Equal(t, 1000, result.Convert.ReplayBatchSize)
	assert.Equal(t, 5, result.Convert.LockTimeout)
	assert.Equal(t, 30, result.Convert.StatementTimeout)
}

func TestGetConvertConfig_PartialConvertValues(t *testing.T) {
	cfg := &Config{
		Partitions: map[string]partition.Configuration{
			"my-events": {
				Schema:         "public",
				Table:          "events",
				PartitionKey:   "created_at",
				Interval:       partition.Daily,
				Retention:      3,
				PreProvisioned: 3,
				CleanupPolicy:  partition.Drop,
				Convert: &partition.ConvertSettings{
					BackfillBatchSize: 5000,
					// Other fields left at zero — defaults should be applied
				},
			},
		},
	}

	result, err := cfg.GetConvertConfig("my-events")
	require.NoError(t, err)
	assert.NotNil(t, result.Convert)
	assert.Equal(t, 5000, result.Convert.BackfillBatchSize)
	assert.Equal(t, 1000, result.Convert.ReplayBatchSize)
	assert.Equal(t, 5, result.Convert.LockTimeout)
	assert.Equal(t, 30, result.Convert.StatementTimeout)
}
