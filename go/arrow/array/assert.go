package array

import (
	"strconv"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/internal/debug"
	"github.com/apache/arrow/go/v13/arrow/memory"
)

func AssertArray(pfx string, arr arrow.Array) {
	pfx += ".AssertArray"
	debug.Assert(arr != nil, pfx+": arr != nil")
	if arr == nil {
		return
	}
	AssertData(pfx, arr.Data().(*Data))
}

func AssertData(pfx string, data *Data) {
	pfx += ".AssertData"
	debug.Assert(data != nil, pfx+": data != nil")
	if data == nil {
		return
	}
	debug.Assert(data.refCount == 1, pfx+": data.refCount="+strconv.Itoa(int(data.refCount)))
	for i, buff := range data.buffers {
		memory.AssertBuffer(pfx+"buff["+strconv.Itoa(i)+"]", buff)
	}
	if data.dictionary != nil {
		AssertData(pfx+".data.dictionary", data.dictionary)
	}
	for i, child := range data.childData {
		AssertData(pfx+"childData["+strconv.Itoa(i)+"]", child.(*Data))
	}
}
