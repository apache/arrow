package arrow

import (
	"github.com/apache/arrow/go/arrow/internal/bitutil"
)

type booleanTraits struct{}

var BooleanTraits booleanTraits

// BytesRequired returns the number of bytes required to store n elements in memory.
func (booleanTraits) BytesRequired(n int) int { return bitutil.CeilByte(n) / 8 }
