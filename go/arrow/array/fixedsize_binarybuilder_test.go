package array

import (
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestFixedSizeBinaryBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dtype := arrow.FixedSizeBinaryType{ByteWidth: 7}
	b := NewFixedSizeBinaryBuilder(mem, dtype)

	b.Append([]byte("1234567"))
	b.AppendNull()
	b.Append([]byte("ABCDEFG"))
	b.AppendNull()

	assert.Equal(t, 4, b.Len(), "unexpected Len()")
	assert.Equal(t, 2, b.NullN(), "unexpected NullN()")

	assert.Equal(t, b.Value(0), []byte("1234567"))
	assert.Equal(t, b.Value(1), []byte{})
	assert.Equal(t, b.Value(2), []byte("ABCDEFG"))
	assert.Equal(t, b.Value(3), []byte{})

	values := [][]byte{
		[]byte("7654321"),
		nil,
		[]byte("AZERTYU"),
	}
	b.AppendValues(values, []bool{true, false, true})

	assert.Equal(t, 7, b.Len(), "unexpected Len()")
	assert.Equal(t, 3, b.NullN(), "unexpected NullN()")

	assert.Equal(t, []byte("7654321"), b.Value(4))
	assert.Equal(t, []byte{}, b.Value(5))
	assert.Equal(t, []byte("AZERTYU"), b.Value(6))

	a := b.NewFixedSizeBinaryArray()

	// check state of builder after NewFixedSizeBinaryArray
	assert.Zero(t, b.Len(), "unexpected ArrayBuilder.Len(), NewFixedSizeBinaryArray did not reset state")
	assert.Zero(t, b.Cap(), "unexpected ArrayBuilder.Cap(), NewFixedSizeBinaryArray did not reset state")
	assert.Zero(t, b.NullN(), "unexpected ArrayBuilder.NullN(), NewFixedSizeBinaryArray did not reset state")

	b.Release()
	a.Release()
}
