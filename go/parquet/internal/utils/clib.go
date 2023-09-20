//go:build !noasm && !appengine
// +build !noasm,!appengine

package utils

import "unsafe"

//go:noescape
func _ClibMemcpy(dst, src unsafe.Pointer, n uint) unsafe.Pointer

//go:noescape
func _ClibMemset(dst unsafe.Pointer, c int, n uint) unsafe.Pointer
