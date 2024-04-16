// Package ppm provides check, provisioning and cleanup methods
package ppm

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
)

type PostgreSQLClient interface {
	CreatePartition(partitionConfiguration postgresql.PartitionConfiguration, partition postgresql.Partition) error
	DeletePartition(partition postgresql.Partition) error
	DetachPartition(partition postgresql.Partition) error
	GetPartitionSettings(postgresql.Table) (postgresql.PartitionSettings, error)
	ListPartitions(table postgresql.Table) ([]postgresql.Partition, error)
	GetVersion() (int64, error)
	GetServerTime() (time.Time, error)
}

type PPM struct {
	ctx        context.Context
	db         PostgreSQLClient
	partitions map[string]postgresql.PartitionConfiguration
	logger     slog.Logger
}

func New(context context.Context, logger slog.Logger, db PostgreSQLClient, partitions map[string]postgresql.PartitionConfiguration) *PPM {
	return &PPM{
		partitions: partitions,
		ctx:        context,
		db:         db,
		logger:     logger,
	}
}

func getExpectedPartitions(partition postgresql.PartitionConfiguration, currentTime time.Time) (partitions []postgresql.Partition, err error) {
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
