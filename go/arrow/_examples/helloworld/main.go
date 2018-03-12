package main

import (
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/math"
	"github.com/apache/arrow/go/arrow/memory"
)

func main() {
	fb := array.NewFloat64Builder(memory.DefaultAllocator)

	fb.AppendValues([]float64{1, 3, 5, 7, 9, 11}, nil)

	vec := fb.Finish()
	math.Float64.Sum(vec)
}
