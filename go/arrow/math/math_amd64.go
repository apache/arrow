// +build !noasm

package math

import (
	"github.com/apache/arrow/go/arrow/internal/cpu"
)

func init() {
	if cpu.X86.HasAVX2 {
		initAVX2()
	} else if cpu.X86.HasSSE42 {
		initSSE4()
	} else {
		initGo()
	}
}

func initAVX2() {
	initFloat64AVX2()
	initInt64AVX2()
	initUint64AVX2()
}

func initSSE4() {
	initFloat64SSE4()
	initInt64SSE4()
	initUint64SSE4()
}

func initGo() {
	initFloat64Go()
	initInt64Go()
	initUint64Go()
}
