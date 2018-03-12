package memory

type GoAllocator struct{}

func NewGoAllocator() *GoAllocator { return &GoAllocator{} }

func (a *GoAllocator) Allocate(size int) []byte {
	buf := make([]byte, size+alignment) // padding for 64-byte alignment
	addr := int(addressOf(buf))
	next := roundUpToMultipleOf64(addr)
	if addr != next {
		shift := next - addr
		return buf[shift : size+shift : size+shift]
	}
	return buf[:size:size]
}

func (a *GoAllocator) Reallocate(size int, b []byte) []byte {
	if size == len(b) {
		return b
	}

	newBuf := a.Allocate(size)
	copy(newBuf, b)
	return newBuf
}

func (a *GoAllocator) Free(b []byte) {}
