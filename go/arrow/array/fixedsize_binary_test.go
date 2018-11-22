package array

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/memory"
)

func TestFixedSizeBinary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := arrow.FixedSizeBinaryType{ByteWidth: 7}
	b := NewFixedSizeBinaryBuilder(mem, dtype)

	values := [][]byte{
		[]byte("7654321"),
		nil,
		[]byte("AZERTYU"),
	}
	b.AppendValues(values, []bool{true, false, true})

	a := b.NewFixedSizeBinaryArray()

	assert.Equal(t, 3, a.Len())
	assert.Equal(t, 1, a.NullN())

	assert.Equal(t, []byte("7654321"), a.Value(0))
	assert.Equal(t, []byte{}, a.Value(1))
	assert.Equal(t, []byte("AZERTYU"), a.Value(2))

	b.Release()
	a.Release()
}
