package pqarrow

import "github.com/apache/arrow/go/v18/parquet/schema"

type LogicalType struct {
	Type schema.LogicalType
	Length int
}

func NewLogicalType() *LogicalType {
	return &LogicalType{
		Type: schema.NoLogicalType{},
		Length: -1,
	}
}
