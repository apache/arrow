// +build !noasm

package memory

import "unsafe"

//go:noescape
func _memset_avx2(buf, len, c unsafe.Pointer)

func memory_memset_avx2(buf []byte, c byte) {
	if len(buf) == 0 {
		return
	}

	var (
		p1 = unsafe.Pointer(&buf[0])
		p2 = unsafe.Pointer(uintptr(len(buf)))
		p3 = unsafe.Pointer(uintptr(c))
	)
	if len(buf) > 2000 || isMultipleOfPowerOf2(len(buf), 256) {
		_memset_avx2(p1, p2, p3)
	} else {
		_memset_sse4(p1, p2, p3)
	}
}
