package ppm

import (
	"errors"
	"fmt"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
)

var ErrPartitionProvisioningFailed = errors.New("partition provisioning failed for one or more partition")

func (p PPM) ProvisioningPartitions() error {
	currentTime := time.Now()
	provisioningFailed := false

	for name, config := range p.partitions {
		p.logger.Info("Provisioning partition", "partition", name)

		if err := p.provisionPartitionsFor(config, currentTime); err != nil {
			provisioningFailed = true
		}
	}

	if provisioningFailed {
		return ErrPartitionProvisioningFailed
	}

	return nil
}

func (p PPM) provisionPartitionsFor(config postgresql.PartitionConfiguration, at time.Time) error {
	provisioningFailed := false

	partitions, err := getExpectedPartitions(config, at)
	if err != nil {
		return fmt.Errorf("could not generate partition to create: %w", err)
	}

	for _, partition := range partitions {
		if err := p.db.CreatePartition(config, partition); err != nil {
			provisioningFailed = true

			p.logger.Error("Failed to create partition", "error", err, "schema", partition.Schema, "table", partition.Name)
		}
	}

	if provisioningFailed {
		return ErrPartitionProvisioningFailed
	}

	return nil
}
