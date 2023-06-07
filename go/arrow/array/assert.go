package array

import (
	"strconv"
	"unsafe"

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

	data := arr.Data().(*Data)
	switch arr := arr.(type) {
	case ExtensionArray:
		assertExtensionArrayData(pfx, data, arr.Storage())
	case *List:
		assertListLike(pfx, arr)
	case *LargeList:
		assertListLike(pfx, arr)
	default:
		AssertData(pfx, data)
	}
}

func AssertDataN(pfx string, data *Data, n int64) {
	pfx += ".AssertDataN(" + strconv.Itoa(int(n)) + ")"
	debug.Assert(data != nil, pfx+": data != nil")
	if data == nil {
		return
	}
	debug.Assert(data.refCount == n, pfx+": data.refCount="+strconv.Itoa(int(data.refCount)))
	for i, buff := range data.buffers {
		memory.AssertBuffer(pfx+".buff["+strconv.Itoa(i)+"]", buff)
	}
	if data.dictionary != nil {
		AssertData(pfx+".data.dictionary", data.dictionary)
	}
	for i, child := range data.childData {
		AssertData(pfx+"childData["+strconv.Itoa(i)+"]", child.(*Data))
	}
}

func AssertData(pfx string, data *Data) {
	pfx += ".AssertData"
	debug.Assert(data != nil, pfx+": data != nil")
	if data == nil {
		return
	}
	debug.Assert(data.refCount == 1, pfx+": data.refCount="+strconv.Itoa(int(data.refCount)))
	for i, buff := range data.buffers {
		memory.AssertBuffer(pfx+".buff["+strconv.Itoa(i)+"]", buff)
	}
	if data.dictionary != nil {
		AssertData(pfx+".data.dictionary", data.dictionary)
	}
	for i, child := range data.childData {
		AssertData(pfx+".childData["+strconv.Itoa(i)+"]", child.(*Data))
	}
}

func assertExtensionArrayData(pfx string, data *Data, storage arrow.Array) {
	debug.Assert(data != nil, pfx+": data != nil")
	if data == nil {
		return
	}
	debug.Assert(data.refCount == 1, pfx+": data.refCount="+strconv.Itoa(int(data.refCount)))

	// we allow the copies to have N references
	copies := make(map[uintptr]*struct {
		n    int64
		buff *memory.Buffer
	})
	for _, b := range data.buffers {
		if b == nil {
			continue
		}
		ptr := uintptr(unsafe.Pointer(b))
		c := copies[ptr]
		if c != nil {
			c.n++
			continue
		}
		copies[ptr] = &struct {
			n    int64
			buff *memory.Buffer
		}{n: 1, buff: b}
	}

	debug.Assert(storage != nil, pfx+": storage != nil")
	if storage == nil {
		return
	}
	debug.Assert(storage.Data() != nil, pfx+": storage.Data() != nil")
	if storage.Data() == nil {
		return
	}

	for _, b := range storage.Data().Buffers() {
		if b == nil {
			continue
		}
		ptr := uintptr(unsafe.Pointer(b))
		c := copies[ptr]
		if c != nil {
			c.n++
			continue
		}
		copies[ptr] = &struct {
			n    int64
			buff *memory.Buffer
		}{n: 1, buff: b}
	}

	for _, c := range copies {
		memory.AssertBufferN(pfx, c.buff, c.n)
	}
}

func AssertTable(pfx string, tbl arrow.Table) {
	pfx += ".AssertTable"
	debug.Assert(tbl != nil, pfx+": table != nil")
	if tbl == nil {
		return
	}

	table := tbl.(*simpleTable)
	debug.Assert(table.refCount == 1, pfx+": table.refCount="+strconv.Itoa(int(table.refCount)))

	for i, col := range table.cols {
		AssertChunked(pfx+".col["+strconv.Itoa(i)+"]", col.Data())
	}
}

func AssertChunked(pfx string, arr *arrow.Chunked) {
	pfx += ".AssertChunked"
	debug.Assert(arr != nil, pfx+": arr != nil")
	debug.Assert(arr.GetRefCount() == 1, pfx+": data.GetRefCount()="+strconv.Itoa(int(arr.GetRefCount())))

	if arr == nil {
		return
	}
	for i, chunk := range arr.Chunks() {
		AssertArray(pfx+".chunk["+strconv.Itoa(i)+"]", chunk)
	}
}

func assertListLike(pfx string, arr ListLike) {
	pfx += ".assertListLike"
	values := arr.ListValues()
	debug.Assert(values != nil, pfx+": values != nil")
	if values == nil {
		return
	}
	// check that the array data is in ref1
	data := arr.Data().(*Data)
	debug.Assert(data != nil, pfx+": data != nil")
	if data == nil {
		return
	}
	debug.Assert(data.refCount == 1, pfx+": data.refCount="+strconv.Itoa(int(data.refCount)))
	for i, buff := range data.buffers {
		memory.AssertBuffer(pfx+".buff["+strconv.Itoa(i)+"]", buff)
	}

	debug.Assert(len(data.childData) == 1, pfx+": len(data.childData) ="+strconv.Itoa(len(data.childData)))
	debug.Assert(data.childData[0] == values.Data(), pfx+": data.childData[0] == values.Data()")
	AssertDataN(pfx+".values", values.Data().(*Data), 2)
}
