package memory_test

import (
	"testing"

	"github.com/apache/arrow/go/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestNewResizableBuffer(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	buf := memory.NewResizableBuffer(mem)
	buf.Retain() // refCount == 2

	exp := 10
	buf.Resize(exp)
	assert.NotNil(t, buf.Bytes())
	assert.Equal(t, exp, len(buf.Bytes()))
	assert.Equal(t, exp, buf.Len())

	buf.Release() // refCount == 1
	assert.NotNil(t, buf.Bytes())

	buf.Release() // refCount == 0
	assert.Nil(t, buf.Bytes())
	assert.Zero(t, buf.Len())
}
