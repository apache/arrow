package memory

import (
	"strconv"

	"github.com/apache/arrow/go/v13/arrow/internal/debug"
)

func AssertBuffer(pfx string, buffer *Buffer) {
	if buffer == nil {
		return
	}
	debug.Assert(buffer.refCount == 1, pfx+": buffer.refCount="+strconv.Itoa(int(buffer.refCount)))
}
