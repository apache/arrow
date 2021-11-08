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

package arrow

import (
	"fmt"
	"strconv"
	"time"
)

type BooleanType struct{}

func (t *BooleanType) ID() Type            { return BOOL }
func (t *BooleanType) Name() string        { return "bool" }
func (t *BooleanType) String() string      { return "bool" }
func (t *BooleanType) Fingerprint() string { return typeFingerprint(t) }

// BitWidth returns the number of bits required to store a single element of this data type in memory.
func (t *BooleanType) BitWidth() int { return 1 }

type FixedSizeBinaryType struct {
	ByteWidth int
}

func (*FixedSizeBinaryType) ID() Type              { return FIXED_SIZE_BINARY }
func (*FixedSizeBinaryType) Name() string          { return "fixed_size_binary" }
func (t *FixedSizeBinaryType) BitWidth() int       { return 8 * t.ByteWidth }
func (t *FixedSizeBinaryType) Fingerprint() string { return typeFingerprint(t) }
func (t *FixedSizeBinaryType) String() string {
	return "fixed_size_binary[" + strconv.Itoa(t.ByteWidth) + "]"
}

type (
	Timestamp int64
	Time32    int32
	Time64    int64
	TimeUnit  int
	Date32    int32
	Date64    int64
	Duration  int64
)

const (
	Nanosecond TimeUnit = iota
	Microsecond
	Millisecond
	Second
)

func (u TimeUnit) Multiplier() time.Duration {
	return [...]time.Duration{time.Nanosecond, time.Microsecond, time.Millisecond, time.Second}[uint(u)&3]
}

func (u TimeUnit) String() string { return [...]string{"ns", "us", "ms", "s"}[uint(u)&3] }

// TimestampType is encoded as a 64-bit signed integer since the UNIX epoch (2017-01-01T00:00:00Z).
// The zero-value is a nanosecond and time zone neutral. Time zone neutral can be
// considered UTC without having "UTC" as a time zone.
type TimestampType struct {
	Unit     TimeUnit
	TimeZone string
}

func (*TimestampType) ID() Type     { return TIMESTAMP }
func (*TimestampType) Name() string { return "timestamp" }
func (t *TimestampType) String() string {
	switch len(t.TimeZone) {
	case 0:
		return "timestamp[" + t.Unit.String() + "]"
	default:
		return "timestamp[" + t.Unit.String() + ", tz=" + t.TimeZone + "]"
	}
}

func (t *TimestampType) Fingerprint() string {
	return fmt.Sprintf("%s%d:%s", typeFingerprint(t)+string(timeUnitFingerprint(t.Unit)), len(t.TimeZone), t.TimeZone)
}

// BitWidth returns the number of bits required to store a single element of this data type in memory.
func (*TimestampType) BitWidth() int { return 64 }

// Time32Type is encoded as a 32-bit signed integer, representing either seconds or milliseconds since midnight.
type Time32Type struct {
	Unit TimeUnit
}

func (*Time32Type) ID() Type         { return TIME32 }
func (*Time32Type) Name() string     { return "time32" }
func (*Time32Type) BitWidth() int    { return 32 }
func (t *Time32Type) String() string { return "time32[" + t.Unit.String() + "]" }
func (t *Time32Type) Fingerprint() string {
	return typeFingerprint(t) + string(timeUnitFingerprint(t.Unit))
}

// Time64Type is encoded as a 64-bit signed integer, representing either microseconds or nanoseconds since midnight.
type Time64Type struct {
	Unit TimeUnit
}

func (*Time64Type) ID() Type         { return TIME64 }
func (*Time64Type) Name() string     { return "time64" }
func (*Time64Type) BitWidth() int    { return 64 }
func (t *Time64Type) String() string { return "time64[" + t.Unit.String() + "]" }
func (t *Time64Type) Fingerprint() string {
	return typeFingerprint(t) + string(timeUnitFingerprint(t.Unit))
}

// DurationType is encoded as a 64-bit signed integer, representing an amount
// of elapsed time without any relation to a calendar artifact.
type DurationType struct {
	Unit TimeUnit
}

func (*DurationType) ID() Type         { return DURATION }
func (*DurationType) Name() string     { return "duration" }
func (*DurationType) BitWidth() int    { return 64 }
func (t *DurationType) String() string { return "duration[" + t.Unit.String() + "]" }
func (t *DurationType) Fingerprint() string {
	return typeFingerprint(t) + string(timeUnitFingerprint(t.Unit))
}

// Float16Type represents a floating point value encoded with a 16-bit precision.
type Float16Type struct{}

func (t *Float16Type) ID() Type            { return FLOAT16 }
func (t *Float16Type) Name() string        { return "float16" }
func (t *Float16Type) String() string      { return "float16" }
func (t *Float16Type) Fingerprint() string { return typeFingerprint(t) }

// BitWidth returns the number of bits required to store a single element of this data type in memory.
func (t *Float16Type) BitWidth() int { return 16 }

// Decimal128Type represents a fixed-size 128-bit decimal type.
type Decimal128Type struct {
	Precision int32
	Scale     int32
}

func (*Decimal128Type) ID() Type      { return DECIMAL128 }
func (*Decimal128Type) Name() string  { return "decimal" }
func (*Decimal128Type) BitWidth() int { return 128 }
func (t *Decimal128Type) String() string {
	return fmt.Sprintf("%s(%d, %d)", t.Name(), t.Precision, t.Scale)
}
func (t *Decimal128Type) Fingerprint() string {
	return fmt.Sprintf("%s[%d,%d,%d]", typeFingerprint(t), t.BitWidth(), t.Precision, t.Scale)
}

// MonthInterval represents a number of months.
type MonthInterval int32

// MonthIntervalType is encoded as a 32-bit signed integer,
// representing a number of months.
type MonthIntervalType struct{}

func (*MonthIntervalType) ID() Type            { return INTERVAL_MONTHS }
func (*MonthIntervalType) Name() string        { return "month_interval" }
func (*MonthIntervalType) String() string      { return "month_interval" }
func (*MonthIntervalType) Fingerprint() string { return typeIDFingerprint(INTERVAL_MONTHS) + "M" }

// BitWidth returns the number of bits required to store a single element of this data type in memory.
func (t *MonthIntervalType) BitWidth() int { return 32 }

// DayTimeInterval represents a number of days and milliseconds (fraction of day).
type DayTimeInterval struct {
	Days         int32 `json:"days"`
	Milliseconds int32 `json:"milliseconds"`
}

// DayTimeIntervalType is encoded as a pair of 32-bit signed integer,
// representing a number of days and milliseconds (fraction of day).
type DayTimeIntervalType struct{}

func (*DayTimeIntervalType) ID() Type            { return INTERVAL_DAY_TIME }
func (*DayTimeIntervalType) Name() string        { return "day_time_interval" }
func (*DayTimeIntervalType) String() string      { return "day_time_interval" }
func (*DayTimeIntervalType) Fingerprint() string { return typeIDFingerprint(INTERVAL_DAY_TIME) + "d" }

// BitWidth returns the number of bits required to store a single element of this data type in memory.
func (t *DayTimeIntervalType) BitWidth() int { return 64 }

// MonthDayNanoInterval represents a number of months, days and nanoseconds (fraction of day).
type MonthDayNanoInterval struct {
	Months      int32 `json:"months"`
	Days        int32 `json:"days"`
	Nanoseconds int64 `json:"nanoseconds"`
}

// MonthDayNanoIntervalType is encoded as two signed 32-bit integers representing
// a number of months and a number of days, followed by a 64-bit integer representing
// the number of nanoseconds since midnight for fractions of a day.
type MonthDayNanoIntervalType struct{}

func (*MonthDayNanoIntervalType) ID() Type       { return INTERVAL_MONTH_DAY_NANO }
func (*MonthDayNanoIntervalType) Name() string   { return "month_day_nano_interval" }
func (*MonthDayNanoIntervalType) String() string { return "month_day_nano_interval" }
func (*MonthDayNanoIntervalType) Fingerprint() string {
	return typeIDFingerprint(INTERVAL_MONTH_DAY_NANO) + "N"
}

// BitWidth returns the number of bits required to store a single element of this data type in memory.
func (*MonthDayNanoIntervalType) BitWidth() int { return 128 }

var (
	FixedWidthTypes = struct {
		Boolean              FixedWidthDataType
		Date32               FixedWidthDataType
		Date64               FixedWidthDataType
		DayTimeInterval      FixedWidthDataType
		Duration_s           FixedWidthDataType
		Duration_ms          FixedWidthDataType
		Duration_us          FixedWidthDataType
		Duration_ns          FixedWidthDataType
		Float16              FixedWidthDataType
		MonthInterval        FixedWidthDataType
		Time32s              FixedWidthDataType
		Time32ms             FixedWidthDataType
		Time64us             FixedWidthDataType
		Time64ns             FixedWidthDataType
		Timestamp_s          FixedWidthDataType
		Timestamp_ms         FixedWidthDataType
		Timestamp_us         FixedWidthDataType
		Timestamp_ns         FixedWidthDataType
		MonthDayNanoInterval FixedWidthDataType
	}{
		Boolean:              &BooleanType{},
		Date32:               &Date32Type{},
		Date64:               &Date64Type{},
		DayTimeInterval:      &DayTimeIntervalType{},
		Duration_s:           &DurationType{Unit: Second},
		Duration_ms:          &DurationType{Unit: Millisecond},
		Duration_us:          &DurationType{Unit: Microsecond},
		Duration_ns:          &DurationType{Unit: Nanosecond},
		Float16:              &Float16Type{},
		MonthInterval:        &MonthIntervalType{},
		Time32s:              &Time32Type{Unit: Second},
		Time32ms:             &Time32Type{Unit: Millisecond},
		Time64us:             &Time64Type{Unit: Microsecond},
		Time64ns:             &Time64Type{Unit: Nanosecond},
		Timestamp_s:          &TimestampType{Unit: Second, TimeZone: "UTC"},
		Timestamp_ms:         &TimestampType{Unit: Millisecond, TimeZone: "UTC"},
		Timestamp_us:         &TimestampType{Unit: Microsecond, TimeZone: "UTC"},
		Timestamp_ns:         &TimestampType{Unit: Nanosecond, TimeZone: "UTC"},
		MonthDayNanoInterval: &MonthDayNanoIntervalType{},
	}

	_ FixedWidthDataType = (*FixedSizeBinaryType)(nil)
)
