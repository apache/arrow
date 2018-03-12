package arrow_test

import (
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/stretchr/testify/assert"
)

// TestTimeUnit_String verifies each time unit matches its string representation.
func TestTimeUnit_String(t *testing.T) {
	tests := []struct {
		u   arrow.TimeUnit
		exp string
	}{
		{arrow.Nanosecond, "ns"},
		{arrow.Microsecond, "µs"},
		{arrow.Millisecond, "ms"},
		{arrow.Second, "s"},
	}
	for _, test := range tests {
		t.Run(test.exp, func(t *testing.T) {
			assert.Equal(t, test.exp, test.u.String())
		})
	}
}
