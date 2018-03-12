package array_test

import (
	"testing"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/internal/testing/tools"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestBooleanBuilder_AppendValues(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	b := array.NewBooleanBuilder(mem)

	exp := tools.Bools(1, 1, 0, 1, 1, 0, 1, 0)
	got := make([]bool, len(exp))

	b.AppendValues(exp, nil)
	a := b.NewBooleanArray()
	b.Release()
	for i := 0; i < a.Len(); i++ {
		got[i] = a.Value(i)
	}
	assert.Equal(t, exp, got)
	a.Release()
}
