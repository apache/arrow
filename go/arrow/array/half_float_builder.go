package array

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/internal/bitutil"
	"github.com/apache/arrow/go/arrow/internal/debug"
	"github.com/apache/arrow/go/arrow/memory"
	"sync/atomic"
)

type HalfFloatBuilder struct {
	builder

	data    *memory.Buffer
	rawData []arrow.Float16
}

func NewHalfFloatBuilder(mem memory.Allocator) *HalfFloatBuilder {
	return &HalfFloatBuilder{builder: builder{refCount: 1, mem: mem}}
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
func (b *HalfFloatBuilder) Release() {
	debug.Assert(atomic.LoadInt64(&b.refCount) > 0, "too many releases")

	if atomic.AddInt64(&b.refCount, -1) == 0 {
		if b.nullBitmap != nil {
			b.nullBitmap.Release()
			b.nullBitmap = nil
		}
		if b.data != nil {
			b.data.Release()
			b.data = nil
			b.rawData = nil
		}
	}
}

func (b *HalfFloatBuilder) Append(v float32) {
	b.Reserve(1)
	bitutil.SetBit(b.nullBitmap.Bytes(), b.length)
	b.rawData[b.length] = arrow.NewFloat16(v)
	b.length++
}

func (b *HalfFloatBuilder) AppendNull() {
	b.Reserve(1)
	b.UnsafeAppendBoolToBitmap(false)
}

func (b *HalfFloatBuilder) UnsafeAppendBoolToBitmap(isValid bool) {
	if isValid {
		bitutil.SetBit(b.nullBitmap.Bytes(), b.length)
	} else {
		b.nulls++
	}
	b.length++
}

// AppendValues will append the values in the v slice. The valid slice determines which values
// in v are valid (not null). The valid slice must either be empty or be equal in length to v. If empty,
// all values in v are appended and considered valid.
func (b *HalfFloatBuilder) AppendValues(v []arrow.Float16, valid []bool) {
	if len(v) != len(valid) && len(valid) != 0 {
		panic("len(v) != len(valid) && len(valid) != 0")
	}

	b.Reserve(len(v))
	if len(v) > 0 {
		arrow.Float16Traits.Copy(b.rawData[b.length:], v)
	}
	b.builder.unsafeAppendBoolsToBitmap(valid, len(v))
}

func (b *HalfFloatBuilder) init(capacity int) {
	b.builder.init(capacity)

	b.data = memory.NewResizableBuffer(b.mem)
	bytesN := arrow.Uint16Traits.BytesRequired(capacity)
	b.data.Resize(bytesN)
	b.rawData = arrow.Float16Traits.CastFromBytes(b.data.Bytes())
}

// Reserve ensures there is enough space for appending n elements
// by checking the capacity and calling Resize if necessary.
func (b *HalfFloatBuilder) Reserve(n int) {
	b.builder.reserve(n, b.Resize)
}

// Resize adjusts the space allocated by b to n elements. If n is greater than b.Cap(),
// additional memory will be allocated. If n is smaller, the allocated memory may reduced.
func (b *HalfFloatBuilder) Resize(n int) {
	nBuilder := n
	if n < minBuilderCapacity {
		n = minBuilderCapacity
	}

	if b.capacity == 0 {
		b.init(n)
	} else {
		b.builder.resize(nBuilder, b.init)
		b.data.Resize(arrow.Float16Traits.BytesRequired(n))
		b.rawData = arrow.Float16Traits.CastFromBytes(b.data.Bytes())
	}
}

// NewArray creates a Uint64 array from the memory buffers used by the builder and resets the Uint64Builder
// so it can be used to build a new array.
func (b *HalfFloatBuilder) NewArray() Interface {
	return b.NewFloat16Array()
}

// NewUint64Array creates a Uint64 array from the memory buffers used by the builder and resets the Uint64Builder
// so it can be used to build a new array.
func (b *HalfFloatBuilder) NewFloat16Array() (a *HalfFloat) {
	data := b.newData()
	a = NewHalfFloatData(data)
	data.Release()
	return
}

func (b *HalfFloatBuilder) newData() (data *Data) {
	bytesRequired := arrow.Float16Traits.BytesRequired(b.length)
	if bytesRequired > 0 && bytesRequired < b.data.Len() {
		// trim buffers
		b.data.Resize(bytesRequired)
	}
	data = NewData(arrow.FixedWidthTypes.Float16, b.length, []*memory.Buffer{b.nullBitmap, b.data}, nil, b.nulls, 0)
	b.reset()

	if b.data != nil {
		b.data.Release()
		b.data = nil
		b.rawData = nil
	}

	return
}
