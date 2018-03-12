// Code generated by type.go.tmpl.
// DO NOT EDIT.

package math

import (
	"github.com/apache/arrow/go/arrow/array"
)

type Float64Funcs struct {
	sum func(a *array.Float64) float64
}

var (
	Float64 Float64Funcs
)

// Sum returns the summation of all elements in a.
func (f Float64Funcs) Sum(a *array.Float64) float64 {
	return f.sum(a)
}

func sum_float64_go(a *array.Float64) float64 {
	acc := float64(0)
	for _, v := range a.Float64Values() {
		acc += v
	}
	return acc
}
