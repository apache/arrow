package csv

import (
	"errors"
	"fmt"
	"github.com/apache/arrow/go/arrow"
)

var (
	ErrMismatchFields = errors.New("arrow/csv: number of records mismatch")
)

func validate(schema *arrow.Schema) {
	for i, f := range schema.Fields() {
		switch ft := f.Type.(type) {
		case *arrow.BooleanType:
		case *arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type, *arrow.Int64Type:
		case *arrow.Uint8Type, *arrow.Uint16Type, *arrow.Uint32Type, *arrow.Uint64Type:
		case *arrow.Float32Type, *arrow.Float64Type:
		case *arrow.StringType:
		default:
			panic(fmt.Errorf("arrow/csv: field %d (%s) has invalid data type %T", i, f.Name, ft))
		}
	}
}