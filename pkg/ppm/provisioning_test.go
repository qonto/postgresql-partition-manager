package ppm_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
	"github.com/qonto/postgresql-partition-manager/pkg/ppm"
	"github.com/qonto/postgresql-partition-manager/pkg/ppm/mocks"
	"github.com/stretchr/testify/mock"
	"gotest.tools/assert"
)

const (
	uuidSchema = "public"
	uuidTable  = "uuid_table"
)

var ErrCreateFailed = errors.New("create failed")

// Partitions created before PPM 1.2.0 have UUIDv7 bounds without the RFC 9562 variant bits
func legacyUUIDPartitions(schema, table string) []postgresql.PartitionResult {
	return []postgresql.PartitionResult{
		{Schema: schema, ParentTable: table, Name: table + "_2025_01", LowerBound: "01941f29-7c00-7000-0000-000000000000", UpperBound: "0194bece-a000-7000-0000-000000000000"},
		{Schema: schema, ParentTable: table, Name: table + "_2025_02", LowerBound: "0194bece-a000-7000-0000-000000000000", UpperBound: "01954f00-b000-7000-0000-000000000000"},
		{Schema: schema, ParentTable: table, Name: table + "_2025_03", LowerBound: "01954f00-b000-7000-0000-000000000000", UpperBound: "0195eea5-d400-7000-0000-000000000000"},
	}
}

func setupUUIDProvisioning(t *testing.T, config partition.Configuration) (*ppm.PPM, *mocks.PostgreSQLClient) {
	t.Helper()

	logger, postgreSQLMock := setupMocks(t)

	postgreSQLMock.On("ListPartitions", config.Schema, config.Table).Return(legacyUUIDPartitions(config.Schema, config.Table), nil).Once()
	postgreSQLMock.On("GetPartitionSettings", config.Schema, config.Table).Return(string(partition.Range), config.PartitionKey, nil)
	postgreSQLMock.On("GetColumnDataType", config.Schema, config.Table, config.PartitionKey).Return(postgresql.UUID, nil)
	postgreSQLMock.On("IsTableExists", config.Schema, mock.Anything).Return(false, nil)
	postgreSQLMock.On("IsPartitionAttached", config.Schema, mock.Anything).Return(false, nil).Maybe()
	postgreSQLMock.On("SetPartitionReplicaIdentity", config.Schema, mock.Anything, config.Table).Return(nil).Maybe()

	workDate := time.Date(2025, 2, 10, 0, 0, 0, 0, time.UTC)

	return ppm.New(context.TODO(), *logger, postgreSQLMock, map[string]partition.Configuration{config.Table: config}, workDate), postgreSQLMock
}

func TestProvisioningReusesExistingUUIDv7Bounds(t *testing.T) {
	config := partition.Configuration{Schema: uuidSchema, Table: uuidTable, PartitionKey: "id", Interval: partition.Monthly, Retention: 3, PreProvisioned: 2}
	provisioner, postgreSQLMock := setupUUIDProvisioning(t, config)

	postgreSQLMock.On("CreateTableLikeTable", config.Schema, mock.Anything, config.Table).Return(nil)
	// Adjacent to the existing set: reuse the existing bound verbatim, otherwise PostgreSQL detects an overlap
	postgreSQLMock.On("AttachPartition", config.Schema, "uuid_table_2024_12", config.Table, "01937f84-5800-7000-8000-000000000000", "01941f29-7c00-7000-0000-000000000000").Return(nil).Once()
	// Not adjacent to the existing set: generated bounds
	postgreSQLMock.On("AttachPartition", config.Schema, "uuid_table_2024_11", config.Table, "0192e505-9000-7000-8000-000000000000", "01937f84-5800-7000-8000-000000000000").Return(nil).Once()
	// Adjacent to the existing set: reuse the existing bound verbatim, otherwise a gap is left between partitions
	postgreSQLMock.On("AttachPartition", config.Schema, "uuid_table_2025_04", config.Table, "0195eea5-d400-7000-0000-000000000000", "01968924-9c00-7000-8000-000000000000").Return(nil).Once()

	err := provisioner.ProvisioningPartitions()

	assert.NilError(t, err)
	postgreSQLMock.AssertExpectations(t)
}

func TestProvisioningDoesNotLeaveGapOnFailure(t *testing.T) {
	config := partition.Configuration{Schema: uuidSchema, Table: uuidTable, PartitionKey: "id", Interval: partition.Monthly, Retention: 3, PreProvisioned: 1}
	provisioner, postgreSQLMock := setupUUIDProvisioning(t, config)

	// The partition adjacent to the existing set is created first. When it fails,
	// the older partitions must not be created, as they would leave a gap.
	postgreSQLMock.On("CreateTableLikeTable", config.Schema, "uuid_table_2024_12", config.Table).Return(ErrCreateFailed).Once()

	err := provisioner.ProvisioningPartitions()

	assert.Assert(t, errors.Is(err, ppm.ErrPartitionProvisioningFailed), "expected ErrPartitionProvisioningFailed")
	postgreSQLMock.AssertNotCalled(t, "CreateTableLikeTable", config.Schema, "uuid_table_2024_11", config.Table)
	postgreSQLMock.AssertNotCalled(t, "AttachPartition", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	postgreSQLMock.AssertExpectations(t)
}
