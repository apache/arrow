package memory

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRoundToPowerOf2(t *testing.T) {
	tests := []struct {
		v, round int
		exp      int
	}{
		{60, 64, 64},
		{122, 64, 128},
		{16, 64, 64},
		{64, 64, 64},
		{13, 8, 16},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("v%d_r%d", test.v, test.round), func(t *testing.T) {
			a := roundToPowerOf2(test.v, test.round)
			assert.Equal(t, test.exp, a)
		})
	}
}

func TestIsMultipleOfPowerOf2(t *testing.T) {
	tests := []struct {
		v, d int
		exp  bool
	}{
		{200, 256, false},
		{256, 256, true},
		{500, 256, false},
		{512, 256, true},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%d_%d_%t", test.v, test.d, test.exp), func(t *testing.T) {
			got := isMultipleOfPowerOf2(test.v, test.d)
			assert.Equal(t, test.exp, got)
		})
	}
}
