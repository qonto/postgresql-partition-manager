package postgresql

type PartitionSettings struct {
	Strategy PartitionStrategy
	Key      string
	KeyType  ColumnType
}

func (p PartitionSettings) SupportedStrategy() bool {
	return p.Strategy == RangePartitionStrategy
}

func (p PartitionSettings) SupportedKeyDataType() bool {
	switch p.KeyType {
	case
		DateColumnType,
		DateTimeColumnType,
		UUIDColumnType:
		return true
	}

	return false
}
