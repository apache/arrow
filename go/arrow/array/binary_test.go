package array

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/memory"
)

func TestBinary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	b := NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)

	values := [][]byte{
		[]byte("AAA"),
		nil,
		[]byte("BBBB"),
	}
	b.AppendValues(values, []bool{true, false, true})

	a := b.NewBinaryArray()

	assert.Equal(t, 3, a.Len())
	assert.Equal(t, 1, a.NullN())

	assert.Equal(t, []byte("AAA"), a.Value(0))
	assert.Equal(t, []byte{}, a.Value(1))
	assert.Equal(t, []byte("BBBB"), a.Value(2))

	b.Release()
	a.Release()
}
