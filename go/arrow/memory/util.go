package memory

import "unsafe"

func roundToPowerOf2(v, round int) int {
	forceCarry := round - 1
	truncateMask := ^forceCarry
	return (v + forceCarry) & truncateMask
}

func roundUpToMultipleOf64(v int) int {
	return roundToPowerOf2(v, 64)
}

func isMultipleOfPowerOf2(v int, d int) bool {
	return (v & (d - 1)) == 0
}

func addressOf(b []byte) uintptr {
	return uintptr(unsafe.Pointer(&b[0]))
}
