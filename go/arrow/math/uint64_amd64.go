// Code generated by type_amd64.go.tmpl.
// DO NOT EDIT.

// +build !noasm

package math

func initUint64AVX2() {
	Uint64.sum = sum_uint64_avx2
}

func initUint64SSE4() {
	Uint64.sum = sum_uint64_sse4
}

func initUint64Go() {
	Uint64.sum = sum_uint64_go
}
