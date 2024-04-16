// Code generated by mockery v2.33.2. DO NOT EDIT.

package mocks

import (
	postgresql "github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
	mock "github.com/stretchr/testify/mock"

	time "time"
)

// PostgreSQLClient is an autogenerated mock type for the PostgreSQLClient type
type PostgreSQLClient struct {
	mock.Mock
}

// CreatePartition provides a mock function with given fields: partitionConfiguration, partition
func (_m *PostgreSQLClient) CreatePartition(partitionConfiguration postgresql.PartitionConfiguration, partition postgresql.Partition) error {
	ret := _m.Called(partitionConfiguration, partition)

	var r0 error
	if rf, ok := ret.Get(0).(func(postgresql.PartitionConfiguration, postgresql.Partition) error); ok {
		r0 = rf(partitionConfiguration, partition)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeletePartition provides a mock function with given fields: partition
func (_m *PostgreSQLClient) DeletePartition(partition postgresql.Partition) error {
	ret := _m.Called(partition)

	var r0 error
	if rf, ok := ret.Get(0).(func(postgresql.Partition) error); ok {
		r0 = rf(partition)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DetachPartition provides a mock function with given fields: partition
func (_m *PostgreSQLClient) DetachPartition(partition postgresql.Partition) error {
	ret := _m.Called(partition)

	var r0 error
	if rf, ok := ret.Get(0).(func(postgresql.Partition) error); ok {
		r0 = rf(partition)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetPartitionSettings provides a mock function with given fields: _a0
func (_m *PostgreSQLClient) GetPartitionSettings(_a0 postgresql.Table) (postgresql.PartitionSettings, error) {
	ret := _m.Called(_a0)

	var r0 postgresql.PartitionSettings
	var r1 error
	if rf, ok := ret.Get(0).(func(postgresql.Table) (postgresql.PartitionSettings, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(postgresql.Table) postgresql.PartitionSettings); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Get(0).(postgresql.PartitionSettings)
	}

	if rf, ok := ret.Get(1).(func(postgresql.Table) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetServerTime provides a mock function with given fields:
func (_m *PostgreSQLClient) GetServerTime() (time.Time, error) {
	ret := _m.Called()

	var r0 time.Time
	var r1 error
	if rf, ok := ret.Get(0).(func() (time.Time, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() time.Time); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(time.Time)
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetVersion provides a mock function with given fields:
func (_m *PostgreSQLClient) GetVersion() (int64, error) {
	ret := _m.Called()

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func() (int64, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() int64); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ListPartitions provides a mock function with given fields: table
func (_m *PostgreSQLClient) ListPartitions(table postgresql.Table) ([]postgresql.Partition, error) {
	ret := _m.Called(table)

	var r0 []postgresql.Partition
	var r1 error
	if rf, ok := ret.Get(0).(func(postgresql.Table) ([]postgresql.Partition, error)); ok {
		return rf(table)
	}
	if rf, ok := ret.Get(0).(func(postgresql.Table) []postgresql.Partition); ok {
		r0 = rf(table)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]postgresql.Partition)
		}
	}

	if rf, ok := ret.Get(1).(func(postgresql.Table) error); ok {
		r1 = rf(table)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewPostgreSQLClient creates a new instance of PostgreSQLClient. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewPostgreSQLClient(t interface {
	mock.TestingT
	Cleanup(func())
},
) *PostgreSQLClient {
	mock := &PostgreSQLClient{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
