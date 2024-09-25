// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package flatbuf

import "strconv"

type SparseTensorIndex byte

const (
	SparseTensorIndexNONE                 SparseTensorIndex = 0
	SparseTensorIndexSparseTensorIndexCOO SparseTensorIndex = 1
	SparseTensorIndexSparseMatrixIndexCSX SparseTensorIndex = 2
	SparseTensorIndexSparseTensorIndexCSF SparseTensorIndex = 3
)

var EnumNamesSparseTensorIndex = map[SparseTensorIndex]string{
	SparseTensorIndexNONE:                 "NONE",
	SparseTensorIndexSparseTensorIndexCOO: "SparseTensorIndexCOO",
	SparseTensorIndexSparseMatrixIndexCSX: "SparseMatrixIndexCSX",
	SparseTensorIndexSparseTensorIndexCSF: "SparseTensorIndexCSF",
}

var EnumValuesSparseTensorIndex = map[string]SparseTensorIndex{
	"NONE":                 SparseTensorIndexNONE,
	"SparseTensorIndexCOO": SparseTensorIndexSparseTensorIndexCOO,
	"SparseMatrixIndexCSX": SparseTensorIndexSparseMatrixIndexCSX,
	"SparseTensorIndexCSF": SparseTensorIndexSparseTensorIndexCSF,
}

func (v SparseTensorIndex) String() string {
	if s, ok := EnumNamesSparseTensorIndex[v]; ok {
		return s
	}
	return "SparseTensorIndex(" + strconv.FormatInt(int64(v), 10) + ")"
}
