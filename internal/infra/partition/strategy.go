package partition

const (
	Range PartitionStrategy = "RANGE"
	List  PartitionStrategy = "LIST"
	Hash  PartitionStrategy = "HASH"
)

type PartitionStrategy string
