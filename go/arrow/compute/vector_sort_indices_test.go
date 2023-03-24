package compute_test

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/compute"
	"github.com/apache/arrow/go/v12/arrow/compute/internal/kernels"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/suite"
)

type SortIndicesSuite struct {
	suite.Suite
	mem *memory.CheckedAllocator

	valueType     arrow.DataType
	jsonData      []string
	expectIndices []int64

	expected compute.Datum
	input    compute.Datum

	ctx context.Context
}

func (suite *SortIndicesSuite) SetupTest() {
	suite.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	suite.ctx = compute.WithAllocator(context.Background(), suite.mem)

	var err error
	inputChunks := make([]arrow.Array, len(suite.jsonData))
	for i, data := range suite.jsonData {
		inputChunks[i], _, err = array.FromJSON(suite.mem,
			suite.valueType, strings.NewReader(data))
		suite.Require().NoError(err)
	}

	exp := array.NewInt64Builder(suite.mem)
	exp.AppendValues(suite.expectIndices, nil)
	arr := exp.NewArray().Data()
	suite.expected = &compute.ArrayDatum{Value: arr}
	chunked := arrow.NewChunked(inputChunks[0].DataType(), inputChunks)
	suite.input = &compute.ChunkedDatum{Value: chunked}

	for i := range inputChunks {
		inputChunks[i].Release()
	}
	exp.Release()
}

func (suite *SortIndicesSuite) TearDownTest() {
	suite.expected.Release()
	suite.input.Release()
	suite.mem.AssertSize(suite.T(), 0)
}

func (suite *SortIndicesSuite) TestSortIndices() {
	result, err := compute.SortIndices(suite.ctx,
		compute.SortIndicesOptions{
			SortKeys: []kernels.SortKey{
				{
					Name:  "a",
					Order: kernels.Ascending,
				},
			},
			NullPlacement: 0,
		}, suite.input)
	suite.Require().NoError(err)
	defer result.Release()

	// assertDatumsEqual(suite.T(), suite.expected, result, nil, nil)
}

func TestSortIndicesFunctions(t *testing.T) {
	// base64 encoded for testing fixed size binary
	const (
		valAba = `YWJh`
		valAbc = `YWJj`
		valAbd = `YWJk`
	)

	tests := []struct {
		name      string
		data      []string
		expect    []int64
		valueType arrow.DataType
	}{
		{"simple int32", []string{`[1, 1, 0, -5, -5, -5, 255, 255]`}, []int64{3, 4, 5, 2, 0, 1, 6, 7}, arrow.PrimitiveTypes.Int32},
		//{"uint32 with nulls", []string{`[null, 1, 1, null, null, 5]`}, arrow.PrimitiveTypes.Uint32},
		//{"boolean", []string{`[true, true, true, false, false]`}, arrow.FixedWidthTypes.Boolean},
		//{"boolean no runs", []string{`[true, false, true, false, true, false, true, false, true]`}, arrow.FixedWidthTypes.Boolean},
		//{"float64 len=1", []string{`[1.0]`}, arrow.PrimitiveTypes.Float64},
		//{"bool chunks", []string{`[true, true]`, `[true, false, null, null, false]`, `[null, null]`}, arrow.FixedWidthTypes.Boolean},
		//{"float32 chunked", []string{`[1, 1, 0, -5, -5]`, `[-5, 255, 255]`}, arrow.PrimitiveTypes.Float32},
		//{"str", []string{`["foo", "foo", "foo", "bar", "bar", "baz", "bar", "bar", "foo", "foo"]`}, arrow.BinaryTypes.String},
		//{"large str", []string{`["foo", "foo", "foo", "bar", "bar", "baz", "bar", "bar", "foo", "foo"]`}, arrow.BinaryTypes.LargeString},
		//{"str chunked", []string{`["foo", "foo", null]`, `["foo", "bar", "bar"]`, `[null, null, "baz"]`, `[null]`}, arrow.BinaryTypes.String},
		//{"empty arrs", []string{`[]`}, arrow.PrimitiveTypes.Float32},
		//{"empty str array", []string{`[]`}, arrow.BinaryTypes.String},
		//{"empty chunked", []string{`[]`, `[]`, `[]`}, arrow.FixedWidthTypes.Boolean},
		//{"fsb", []string{`["` + valAba + `", "` + valAba + `", null, "` + valAbc + `", "` + valAbd + `", "` + valAbd + `", "` + valAbd + `"]`}, &arrow.FixedSizeBinaryType{ByteWidth: 3}},
		//{"fsb chunked", []string{`["` + valAba + `", "` + valAba + `", null]`, `["` + valAbc + `", "` + valAbd + `", "` + valAbd + `", "` + valAbd + `"]`, `[]`}, &arrow.FixedSizeBinaryType{ByteWidth: 3}}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			suite.Run(t, &SortIndicesSuite{
				valueType:     tt.valueType,
				jsonData:      tt.data,
				expectIndices: tt.expect,
			})
		})
	}
}
