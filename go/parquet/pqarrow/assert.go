package pqarrow

import (
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
)

func assertBuildArray(pfx string, arr *arrow.Chunked, err error) {
	if err != nil {
		return
	}
	array.AssertChunked(pfx+".assertBuildArray", arr)
}

func assertReadTable(pfx string, arr arrow.Table, err error) {
	if err != nil {
		return
	}
	array.AssertTable(pfx+".assertReadTable", arr)
}
