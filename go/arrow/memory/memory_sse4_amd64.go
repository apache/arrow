// +build !noasm

package memory

import "unsafe"

//go:noescape
func _memset_sse4(buf, len, c unsafe.Pointer)

func memory_memset_sse4(buf []byte, c byte) {
	if len(buf) == 0 {
		return
	}
	_memset_sse4(unsafe.Pointer(&buf[0]), unsafe.Pointer(uintptr(len(buf))), unsafe.Pointer(uintptr(c)))
}
