package tools_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/arrow/internal/testing/tools"
	"github.com/stretchr/testify/assert"
)

func TestIntsToBitsLSB(t *testing.T) {
	tests := []struct {
		in  int32
		exp byte
	}{
		{0x11001010, 0x53},
		{0x00001111, 0xf0},
		{0x11110000, 0x0f},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%08x", test.in), func(t *testing.T) {
			got := tools.IntsToBitsLSB(test.in)
			assert.Equal(t, []byte{test.exp}, got)
		})
	}
}
