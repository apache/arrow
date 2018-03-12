package memory

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func isAlignedTo(addr, alignment int) bool {
	return addr&(alignment-1) == 0
}

func TestGoAllocator_Allocate(t *testing.T) {
	tests := []struct {
		name string
		sz   int
	}{
		{"lt alignment", 33},
		{"gt alignment unaligned", 65},
		{"eq alignment", 64},
		{"large unaligned", 4097},
		{"large aligned", 8192},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a := &GoAllocator{}
			buf := a.Allocate(test.sz)
			addr := addressOf(buf)
			assert.True(t, isAlignedTo(int(addr), alignment))
			assert.Equal(t, test.sz, len(buf), "invalid len")
			assert.Equal(t, test.sz, cap(buf), "invalid cap")
		})
	}
}

func TestGoAllocator_Reallocate(t *testing.T) {
	tests := []struct {
		name     string
		sz1, sz2 int
	}{
		{"smaller", 200, 100},
		{"same", 200, 200},
		{"larger", 200, 300},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a := &GoAllocator{}
			buf := a.Allocate(test.sz1)
			for i := range buf {
				buf[i] = byte(i & 0xff)
			}

			exp := make([]byte, test.sz2)
			copy(exp, buf)

			newBuf := a.Reallocate(test.sz2, buf)
			assert.Equal(t, exp, newBuf)
		})
	}
}
