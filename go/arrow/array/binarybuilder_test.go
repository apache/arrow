package array_test

import (
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestBinaryBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	ab := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)

	exp := [][]byte{[]byte("foo"), []byte("bar"), nil, []byte("sydney"), []byte("cameron")}
	for _, v := range exp {
		if v == nil {
			ab.AppendNull()
		} else {
			ab.Append(v)
		}
	}

	assert.Equal(t, len(exp), ab.Len(), "unexpected Len()")
	assert.Equal(t, 1, ab.NullN(), "unexpected NullN()")

	for i, v := range exp {
		if v == nil {
			v = []byte{}
		}
		assert.Equal(t, v, ab.Value(i), "unexpected BinaryArrayBuilder.Value(%d)", i)
	}

	ar := ab.NewBinaryArray()
	ab.Release()
	ar.Release()

	// check state of builder after NewBinaryArray
	assert.Zero(t, ab.Len(), "unexpected ArrayBuilder.Len(), NewBinaryArray did not reset state")
	assert.Zero(t, ab.Cap(), "unexpected ArrayBuilder.Cap(), NewBinaryArray did not reset state")
	assert.Zero(t, ab.NullN(), "unexpected ArrayBuilder.NullN(), NewBinaryArray did not reset state")
}
