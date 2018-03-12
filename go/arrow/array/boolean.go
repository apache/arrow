package array

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/internal/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
)

// A type which represents an immutable sequence of boolean values.
type Boolean struct {
	array
	values []byte
}

// NewBoolean creates a boolean array from the data memory.Buffer and contains length elements.
// The nullBitmap buffer can be nil of there are no null values.
// If nullN is not known, use UnknownNullCount to calculate the value of NullN at runtime from the nullBitmap buffer.
func NewBoolean(length int, data *memory.Buffer, nullBitmap *memory.Buffer, nullN int) *Boolean {
	return NewBooleanData(NewData(arrow.FixedWidthTypes.Boolean, length, []*memory.Buffer{nullBitmap, data}, nullN))
}

func NewBooleanData(data *Data) *Boolean {
	a := &Boolean{}
	a.refCount = 1
	a.setData(data)
	return a
}

func (a *Boolean) Value(i int) bool { return bitutil.BitIsSet(a.values, i) }

func (a *Boolean) setData(data *Data) {
	a.array.setData(data)
	vals := data.buffers[1]
	if vals != nil {
		a.values = vals.Bytes()
	}
}
