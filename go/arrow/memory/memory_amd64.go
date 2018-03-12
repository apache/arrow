// +build !noasm

package memory

import (
	"github.com/apache/arrow/go/arrow/internal/cpu"
)

func init() {
	if cpu.X86.HasAVX2 {
		memset = memory_memset_avx2
	} else if cpu.X86.HasSSE42 {
		memset = memory_memset_sse4
	} else {
		memset = memory_memset_go
	}
}
