package memory

type CheckedAllocator struct {
	mem  Allocator
	base int
	sz   int
}

func NewCheckedAllocator(mem Allocator) *CheckedAllocator {
	return &CheckedAllocator{mem: mem}
}

func (a *CheckedAllocator) Allocate(size int) []byte {
	a.sz += size
	return a.mem.Allocate(size)
}

func (a *CheckedAllocator) Reallocate(size int, b []byte) []byte {
	a.sz += size - len(b)
	return a.mem.Reallocate(size, b)
}

func (a *CheckedAllocator) Free(b []byte) {
	a.sz -= len(b)
	a.mem.Free(b)
}

type TestingT interface {
	Errorf(format string, args ...interface{})
	Helper()
}

func (a *CheckedAllocator) AssertSize(t TestingT, sz int) {
	if a.sz != sz {
		t.Helper()
		t.Errorf("invalid memory size exp=%d, got=%d", sz, a.sz)
	}
}

type CheckedAllocatorScope struct {
	alloc *CheckedAllocator
	sz    int
}

func NewCheckedAllocatorScope(alloc *CheckedAllocator) *CheckedAllocatorScope {
	return &CheckedAllocatorScope{alloc: alloc, sz: alloc.sz}
}

func (c *CheckedAllocatorScope) CheckSize(t TestingT) {
	if c.sz != c.alloc.sz {
		t.Helper()
		t.Errorf("invalid memory size exp=%d, got=%d", c.sz, c.alloc.sz)
	}
}
