// Package ppm provides check, provisioning and cleanup methods
package ppm

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
)

type PostgreSQLClient interface {
	ListPartitions(schema, table string) (partitions []postgresql.PartitionResult, err error)
	GetEngineVersion() (int64, error)
	GetServerTime() (time.Time, error)
	IsTableExists(schema, table string) (bool, error)
	IsPartitionAttached(schema, table string) (bool, error)
	AttachPartition(schema, table, parent, lowerBound, upperBound string) error
	CreateTableLikeTable(schema, table, parent string) error
	GetColumnDataType(schema, table, column string) (postgresql.ColumnType, error)
	GetPartitionSettings(schema, table string) (strategy, key string, err error)
	DropTable(schema, table string) error
	DetachPartitionConcurrently(schema, table, parent string) error
	FinalizePartitionDetach(schema, table, parent string) error
}

type PPM struct {
	ctx        context.Context
	db         PostgreSQLClient
	partitions map[string]partition.Configuration
	logger     slog.Logger
	workDate   time.Time
}

func New(context context.Context, logger slog.Logger, db PostgreSQLClient, partitions map[string]partition.Configuration, workDate time.Time) *PPM {
	return &PPM{
		partitions: partitions,
		ctx:        context,
		db:         db,
		logger:     logger,
		workDate:   workDate,
	}
}

func getExpectedPartitions(partition partition.Configuration, currentTime time.Time) (partitions []partition.Partition, err error) {
	retentions, err := partition.GetRetentionPartitions(currentTime)
	if err != nil {
		return partitions, fmt.Errorf("could not generate retention partitions: %w", err)
	}

	current, err := partition.GeneratePartition(currentTime)
	if err != nil {
		return partitions, fmt.Errorf("could not generate current partition: %w", err)
	}

	future, err := partition.GetPreProvisionedPartitions(currentTime)
	if err != nil {
		return partitions, fmt.Errorf("could not generate preProvisioned partition: %w", err)
	}

	partitions = append(partitions, retentions...)
	partitions = append(partitions, current)
	partitions = append(partitions, future...)

	return
}
