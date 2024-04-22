package partition

const (
	RangePartitionStrategy PartitionStrategy = "RANGE"
	ListPartitionStrategy  PartitionStrategy = "LIST"
	HashPartitionStrategy  PartitionStrategy = "HASH"
)

type PartitionStrategy string
