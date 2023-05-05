package array

import (
	"testing"

	"github.com/apache/arrow/go/v13/arrow/float16"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestFloat16_ValueStr(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	b := NewFloat16Builder(mem)
	defer b.Release()

	b.Append(float16.New(1))
	b.Append(float16.New(2))
	b.Append(float16.New(3))
	b.AppendNull()
	b.Append(float16.New(5))
	b.Append(float16.New(6))
	b.AppendNull()
	b.Append(float16.New(8))
	b.Append(float16.New(9))
	b.Append(float16.New(10))

	arr := b.NewArray().(*Float16)
	defer arr.Release()

	// 2. create array via AppendValueFromString
	b1 := NewFloat16Builder(mem)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*Float16)
	defer arr1.Release()

	assert.Equal(t, arr.Len(), arr1.Len())
	for i := 0; i < arr.Len(); i++ {
		assert.Equal(t, arr.IsValid(i), arr1.IsValid(i))
		assert.Equal(t, arr.ValueStr(i), arr1.ValueStr(i))
	}
}
