package ppm

import "github.com/qonto/postgresql-partition-manager/internal/infra/partition"

// skipConversionInProgress checks if a table has an active conversion in progress
// and should be skipped by the run commands (check, provisioning, cleanup).
// It uses fail-open semantics: if the check fails, the table is NOT skipped.
func (p *PPM) skipConversionInProgress(name string, config partition.Configuration) bool {
	inProgress, err := p.db.IsConversionInProgress(config.Schema, config.Table)
	if err != nil {
		// Fail-open: if we can't check, don't skip (log the error)
		p.logger.Error("Failed to check conversion status, proceeding with partition",
			"partition", name,
			"schema", config.Schema,
			"table", config.Table,
			"error", err,
		)

		return false
	}

	if inProgress {
		p.logger.Warn("Skipping partition: conversion in progress",
			"partition", name,
			"schema", config.Schema,
			"table", config.Table,
		)

		return true
	}

	return false
}
