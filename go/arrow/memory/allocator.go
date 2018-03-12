package memory

const (
	alignment = 64
)

type Allocator interface {
	Allocate(size int) []byte
	Reallocate(size int, b []byte) []byte
	Free(b []byte)
}

// DefaultAllocator is a default implementation of Allocator and can be used anywhere
// an Allocator is required.
//
// DefaultAllocator is safe to use from multiple goroutines.
var DefaultAllocator Allocator = NewGoAllocator()
