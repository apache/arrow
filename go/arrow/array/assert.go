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
	if arr, ok := arr.(ExtensionArray); ok {
		assertExtensionArray(pfx, arr)
		return
	}
	AssertData(pfx, arr.Data().(*Data))
}

func AssertDataN(pfx string, data *Data, n int64) {
	pfx += ".AssertDataN(" + strconv.Itoa(int(n)) + ")"
	debug.Assert(data != nil, pfx+": data != nil")
	if data == nil {
		return
	}
	debug.Assert(data.refCount == n, pfx+": data.refCount="+strconv.Itoa(int(data.refCount)))
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
		AssertData(pfx+".childData["+strconv.Itoa(i)+"]", child.(*Data))
	}
}

func assertExtensionArray(pfx string, arr ExtensionArray) {
	debug.Assert(arr.Data() != nil, pfx+": arr.Data() != nil")
	if arr.Data() == nil {
		return
	}

	data := arr.Data().(*Data)
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
	for _, b := range arr.Storage().Data().Buffers() {
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
