package pqarrow

import (
	"strconv"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/parquet/internal/debug"
)

func assertBuildArray(pfx string, arr *arrow.Chunked, err error) {
	if err != nil {
		return
	}
	pfx += ".assertBuildArray"
	debug.Assert(arr != nil, pfx+": arr != nil")
	if arr == nil {
		return
	}
	for i, chunk := range arr.Chunks() {
		array.AssertArray(pfx+".chunk["+strconv.Itoa(i)+"]", chunk)
	}
}
