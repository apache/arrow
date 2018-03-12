package array

import "github.com/apache/arrow/go/arrow/memory"

type byteBufferBuilder struct {
	bufferBuilder
}

func newByteBufferBuilder(mem memory.Allocator) *byteBufferBuilder {
	return &byteBufferBuilder{bufferBuilder: bufferBuilder{refCount: 1, mem: mem}}
}

func (b *byteBufferBuilder) Values() []byte   { return b.Bytes() }
func (b *byteBufferBuilder) Value(i int) byte { return b.bytes[i] }
