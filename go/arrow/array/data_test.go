package array

import (
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestDataReset(t *testing.T) {
	var (
		buffers1 = make([]*memory.Buffer, 0, 3)
		buffers2 = make([]*memory.Buffer, 0, 3)
	)
	for i := 0; i < cap(buffers1); i++ {
		buffers1 = append(buffers1, memory.NewBufferBytes([]byte("some-bytes1")))
		buffers2 = append(buffers2, memory.NewBufferBytes([]byte("some-bytes2")))
	}

	data := NewData(&arrow.StringType{}, 10, buffers1, nil, 0, 0)
	data.Reset(&arrow.Int64Type{}, 5, buffers2, nil, 1, 2)

	for i := 0; i < 2; i++ {
		assert.Equal(t, buffers2, data.Buffers())
		assert.Equal(t, &arrow.Int64Type{}, data.DataType())
		assert.Equal(t, 1, data.NullN())
		assert.Equal(t, 2, data.Offset())
		assert.Equal(t, 5, data.Len())

		// Make sure it works when resetting the data with its own buffers (new buffers are retained
		// before old ones are released.)
		data.Reset(&arrow.Int64Type{}, 5, data.Buffers(), nil, 1, 2)
	}
}
