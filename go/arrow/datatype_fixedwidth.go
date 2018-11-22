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

type BooleanType struct{}

func (t *BooleanType) ID() Type     { return BOOL }
func (t *BooleanType) Name() string { return "bool" }

// BitWidth returns the number of bits required to store a single element of this data type in memory.
func (t *BooleanType) BitWidth() int { return 1 }

type FixedSizeBinaryType struct {
	ByteWidth int
}

func (*FixedSizeBinaryType) ID() Type        { return FIXED_SIZE_BINARY }
func (*FixedSizeBinaryType) Name() string    { return "fixed_size_binary" }
func (t *FixedSizeBinaryType) BitWidth() int { return 8 * t.ByteWidth }

type (
	Timestamp int64
	Time32    int32
	Time64    int64
	TimeUnit  int
)

const (
	Nanosecond TimeUnit = iota
	Microsecond
	Millisecond
	Second
)

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

// BitWidth returns the number of bits required to store a single element of this data type in memory.
func (*TimestampType) BitWidth() int { return 64 }

// Time32Type is encoded as a 32-bit signed integer, representing either seconds or milliseconds since midnight.
type Time32Type struct {
	Unit TimeUnit
}

func (*Time32Type) ID() Type      { return TIME32 }
func (*Time32Type) Name() string  { return "time32" }
func (*Time32Type) BitWidth() int { return 32 }

// Time64Type is encoded as a 64-bit signed integer, representing either microseconds or nanoseconds since midnight.
type Time64Type struct {
	Unit TimeUnit
}

func (*Time64Type) ID() Type      { return TIME64 }
func (*Time64Type) Name() string  { return "time64" }
func (*Time64Type) BitWidth() int { return 64 }

var (
	FixedWidthTypes = struct {
		Boolean  FixedWidthDataType
		Time32s  FixedWidthDataType
		Time32ms FixedWidthDataType
		Time64us FixedWidthDataType
		Time64ns FixedWidthDataType
	}{
		Boolean:  &BooleanType{},
		Time32s:  &Time32Type{Unit: Second},
		Time32ms: &Time32Type{Unit: Millisecond},
		Time64us: &Time64Type{Unit: Microsecond},
		Time64ns: &Time64Type{Unit: Nanosecond},
	}

	_ FixedWidthDataType = (*FixedSizeBinaryType)(nil)
)
