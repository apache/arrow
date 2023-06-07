package memory

import (
	"strconv"

	"github.com/apache/arrow/go/v13/arrow/internal/debug"
)

func AssertBufferN(pfx string, buffer *Buffer, n int64) {
	pfx += ".AssertBufferN"
	if buffer == nil {
		return
	}
	debug.Assert(buffer.refCount == n,
		pfx+": buffer.refCount="+strconv.Itoa(int(buffer.refCount))+" != "+strconv.Itoa(int(n)))
}

func AssertBuffer(pfx string, buffer *Buffer) {
	pfx += ".AssertBuffer"
	if buffer == nil {
		return
	}
	debug.Assert(buffer.refCount == 1, pfx+": buffer.refCount="+strconv.Itoa(int(buffer.refCount)))
}
