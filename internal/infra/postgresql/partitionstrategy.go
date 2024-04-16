package postgresql

const (
	RangePartitionStrategy PartitionStrategy = "RANGE"
	ListPartitionStrategy  PartitionStrategy = "LIST"
	HashPartitionStrategy  PartitionStrategy = "HASH"
)

type PartitionStrategy string
