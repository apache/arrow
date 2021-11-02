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
			if got, want := dt.BitWidth(), 128; got != want {
				t.Fatalf("invalid bitwidth: got=%d, want=%d", got, want)
			}

			if got, want := dt.ID(), arrow.DECIMAL128; got != want {
				t.Fatalf("invalid type ID: got=%v, want=%v", got, want)
			}

			if got, want := dt.String(), tc.want; got != want {
				t.Fatalf("invalid stringer: got=%q, want=%q", got, want)
			}
		})
	}
}

func TestFixedSizeBinaryType(t *testing.T) {
	for _, tc := range []struct {
		byteWidth int
		want      string
	}{
		{1, "fixed_size_binary[1]"},
		{8, "fixed_size_binary[8]"},
		{100, "fixed_size_binary[100]"},
		{100000000, "fixed_size_binary[100000000]"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			dt := arrow.FixedSizeBinaryType{tc.byteWidth}
			if got, want := dt.BitWidth(), 8*tc.byteWidth; got != want {
				t.Fatalf("invalid bitwidth: got=%d, want=%d", got, want)
			}

			if got, want := dt.Name(), "fixed_size_binary"; got != want {
				t.Fatalf("invalid type name: got=%q, want=%q", got, want)
			}

			if got, want := dt.ID(), arrow.FIXED_SIZE_BINARY; got != want {
				t.Fatalf("invalid type ID: got=%v, want=%v", got, want)
			}

			if got, want := dt.String(), tc.want; got != want {
				t.Fatalf("invalid type stringer: got=%q, want=%q", got, want)
			}
		})
	}
}

func TestTimestampType(t *testing.T) {
	for _, tc := range []struct {
		unit     arrow.TimeUnit
		timeZone string
		want     string
	}{
		{arrow.Nanosecond, "CST", "timestamp[ns, tz=CST]"},
		{arrow.Microsecond, "EST", "timestamp[us, tz=EST]"},
		{arrow.Millisecond, "UTC", "timestamp[ms, tz=UTC]"},
		{arrow.Second, "", "timestamp[s]"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			dt := arrow.TimestampType{tc.unit, tc.timeZone}
			if got, want := dt.BitWidth(), 64; got != want {
				t.Fatalf("invalid bitwidth: got=%d, want=%d", got, want)
			}

			if got, want := dt.Name(), "timestamp"; got != want {
				t.Fatalf("invalid type name: got=%q, want=%q", got, want)
			}

			if got, want := dt.ID(), arrow.TIMESTAMP; got != want {
				t.Fatalf("invalid type ID: got=%v, want=%v", got, want)
			}

			if got, want := dt.String(), tc.want; got != want {
				t.Fatalf("invalid type stringer: got=%q, want=%q", got, want)
			}
		})
	}
}

func TestTime32Type(t *testing.T) {
	for _, tc := range []struct {
		unit arrow.TimeUnit
		want string
	}{
		{arrow.Millisecond, "time32[ms]"},
		{arrow.Second, "time32[s]"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			dt := arrow.Time32Type{tc.unit}
			if got, want := dt.BitWidth(), 32; got != want {
				t.Fatalf("invalid bitwidth: got=%d, want=%d", got, want)
			}

			if got, want := dt.Name(), "time32"; got != want {
				t.Fatalf("invalid type name: got=%q, want=%q", got, want)
			}

			if got, want := dt.ID(), arrow.TIME32; got != want {
				t.Fatalf("invalid type ID: got=%v, want=%v", got, want)
			}

			if got, want := dt.String(), tc.want; got != want {
				t.Fatalf("invalid type stringer: got=%q, want=%q", got, want)
			}
		})
	}
}

func TestTime64Type(t *testing.T) {
	for _, tc := range []struct {
		unit arrow.TimeUnit
		want string
	}{
		{arrow.Nanosecond, "time64[ns]"},
		{arrow.Microsecond, "time64[us]"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			dt := arrow.Time64Type{tc.unit}
			if got, want := dt.BitWidth(), 64; got != want {
				t.Fatalf("invalid bitwidth: got=%d, want=%d", got, want)
			}

			if got, want := dt.Name(), "time64"; got != want {
				t.Fatalf("invalid type name: got=%q, want=%q", got, want)
			}

			if got, want := dt.ID(), arrow.TIME64; got != want {
				t.Fatalf("invalid type ID: got=%v, want=%v", got, want)
			}

			if got, want := dt.String(), tc.want; got != want {
				t.Fatalf("invalid type stringer: got=%q, want=%q", got, want)
			}
		})
	}
}

func TestDurationType(t *testing.T) {
	for _, tc := range []struct {
		unit arrow.TimeUnit
		want string
	}{
		{arrow.Nanosecond, "duration[ns]"},
		{arrow.Microsecond, "duration[us]"},
		{arrow.Millisecond, "duration[ms]"},
		{arrow.Second, "duration[s]"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			dt := arrow.DurationType{tc.unit}
			if got, want := dt.BitWidth(), 64; got != want {
				t.Fatalf("invalid bitwidth: got=%d, want=%d", got, want)
			}

			if got, want := dt.Name(), "duration"; got != want {
				t.Fatalf("invalid type name: got=%q, want=%q", got, want)
			}

			if got, want := dt.ID(), arrow.DURATION; got != want {
				t.Fatalf("invalid type ID: got=%v, want=%v", got, want)
			}

			if got, want := dt.String(), tc.want; got != want {
				t.Fatalf("invalid type stringer: got=%q, want=%q", got, want)
			}
		})
	}
}

func TestBooleanType(t *testing.T) {
	dt := arrow.BooleanType{}
	if got, want := dt.BitWidth(), 1; got != want {
		t.Fatalf("invalid bitwidth: got=%d, want=%d", got, want)
	}

	if got, want := dt.Name(), "bool"; got != want {
		t.Fatalf("invalid type name: got=%q, want=%q", got, want)
	}

	if got, want := dt.ID(), arrow.BOOL; got != want {
		t.Fatalf("invalid type ID: got=%v, want=%v", got, want)
	}

	if got, want := dt.String(), "bool"; got != want {
		t.Fatalf("invalid type stringer: got=%q, want=%q", got, want)
	}
}

func TestFloat16Type(t *testing.T) {
	dt := arrow.Float16Type{}
	if got, want := dt.BitWidth(), 16; got != want {
		t.Fatalf("invalid bitwidth: got=%d, want=%d", got, want)
	}

	if got, want := dt.Name(), "float16"; got != want {
		t.Fatalf("invalid type name: got=%q, want=%q", got, want)
	}

	if got, want := dt.ID(), arrow.FLOAT16; got != want {
		t.Fatalf("invalid type ID: got=%v, want=%v", got, want)
	}

	if got, want := dt.String(), "float16"; got != want {
		t.Fatalf("invalid type stringer: got=%q, want=%q", got, want)
	}
}

func TestDayTimeIntervalType(t *testing.T) {
	dt := arrow.DayTimeIntervalType{}
	if got, want := dt.BitWidth(), 64; got != want {
		t.Fatalf("invalid bitwidth: got=%d, want=%d", got, want)
	}

	if got, want := dt.Name(), "day_time_interval"; got != want {
		t.Fatalf("invalid type name: got=%q, want=%q", got, want)
	}

	if got, want := dt.ID(), arrow.INTERVAL_DAY_TIME; got != want {
		t.Fatalf("invalid type ID: got=%v, want=%v", got, want)
	}

	if got, want := dt.String(), "day_time_interval"; got != want {
		t.Fatalf("invalid type stringer: got=%q, want=%q", got, want)
	}
}

func TestMonthIntervalType(t *testing.T) {
	dt := arrow.MonthIntervalType{}
	if got, want := dt.BitWidth(), 32; got != want {
		t.Fatalf("invalid bitwidth: got=%d, want=%d", got, want)
	}

	if got, want := dt.Name(), "month_interval"; got != want {
		t.Fatalf("invalid type name: got=%q, want=%q", got, want)
	}

	if got, want := dt.ID(), arrow.INTERVAL_MONTHS; got != want {
		t.Fatalf("invalid type ID: got=%v, want=%v", got, want)
	}

	if got, want := dt.String(), "month_interval"; got != want {
		t.Fatalf("invalid type stringer: got=%q, want=%q", got, want)
	}
}
