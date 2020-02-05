// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
		{arrow.Microsecond, "us"},
		{arrow.Millisecond, "ms"},
		{arrow.Second, "s"},
	}
	for _, test := range tests {
		t.Run(test.exp, func(t *testing.T) {
			assert.Equal(t, test.exp, test.u.String())
		})
	}
}

func TestDecimal128Type(t *testing.T) {
	for _, tc := range []struct {
		precision int32
		scale     int32
		want      string
	}{
		{1, 10, "decimal(1, 10)"},
		{10, 10, "decimal(10, 10)"},
		{10, 1, "decimal(10, 1)"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			dt := arrow.Decimal128Type{Precision: tc.precision, Scale: tc.scale}
			if got, want := dt.BitWidth(), 16; got != want {
				t.Fatalf("invalid bitwidth: got=%d, want=%d", got, want)
			}

			if got, want := dt.ID(), arrow.DECIMAL; got != want {
				t.Fatalf("invalid type ID: got=%v, want=%v", got, want)
			}

			if got, want := dt.String(), tc.want; got != want {
				t.Fatalf("invalid stringer: got=%q, want=%q", got, want)
			}
		})
	}
}
