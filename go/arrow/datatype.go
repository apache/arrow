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
	"hash/maphash"

	"github.com/apache/arrow/go/arrow/internal/debug"
)

// Type is a logical type. They can be expressed as
// either a primitive physical type (bytes or bits of some fixed size), a
// nested type consisting of other data types, or another data type (e.g. a
// timestamp encoded as an int64)
type Type int

const (
	// NULL type having no physical storage
	NULL Type = iota

	// BOOL is a 1 bit, LSB bit-packed ordering
	BOOL

	// UINT8 is an Unsigned 8-bit little-endian integer
	UINT8

	// INT8 is a Signed 8-bit little-endian integer
	INT8

	// UINT16 is an Unsigned 16-bit little-endian integer
	UINT16

	// INT16 is a Signed 16-bit little-endian integer
	INT16

	// UINT32 is an Unsigned 32-bit little-endian integer
	UINT32

	// INT32 is a Signed 32-bit little-endian integer
	INT32

	// UINT64 is an Unsigned 64-bit little-endian integer
	UINT64

	// INT64 is a Signed 64-bit little-endian integer
	INT64

	// FLOAT16 is a 2-byte floating point value
	FLOAT16

	// FLOAT32 is a 4-byte floating point value
	FLOAT32

	// FLOAT64 is an 8-byte floating point value
	FLOAT64

	// STRING is a UTF8 variable-length string
	STRING

	// BINARY is a Variable-length byte type (no guarantee of UTF8-ness)
	BINARY

	// FIXED_SIZE_BINARY is a binary where each value occupies the same number of bytes
	FIXED_SIZE_BINARY

	// DATE32 is int32 days since the UNIX epoch
	DATE32

	// DATE64 is int64 milliseconds since the UNIX epoch
	DATE64

	// TIMESTAMP is an exact timestamp encoded with int64 since UNIX epoch
	// Default unit millisecond
	TIMESTAMP

	// TIME32 is a signed 32-bit integer, representing either seconds or
	// milliseconds since midnight
	TIME32

	// TIME64 is a signed 64-bit integer, representing either microseconds or
	// nanoseconds since midnight
	TIME64

	// INTERVAL_MONTHS is YEAR_MONTH interval in SQL style
	INTERVAL_MONTHS

	// INTERVAL_DAY_TIME is DAY_TIME in SQL Style
	INTERVAL_DAY_TIME

	// DECIMAL128 is a precision- and scale-based decimal type. Storage type depends on the
	// parameters.
	DECIMAL128

	// DECIMAL256 is a precision and scale based decimal type, with 256 bit max. not yet implemented
	DECIMAL256

	// LIST is a list of some logical data type
	LIST

	// STRUCT of logical types
	STRUCT

	// SPARSE_UNION of logical types. not yet implemented
	SPARSE_UNION

	// DENSE_UNION of logical types. not yet implemented
	DENSE_UNION

	// DICTIONARY aka Category type
	DICTIONARY

	// MAP is a repeated struct logical type
	MAP

	// Custom data type, implemented by user
	EXTENSION

	// Fixed size list of some logical type
	FIXED_SIZE_LIST

	// Measure of elapsed time in either seconds, milliseconds, microseconds
	// or nanoseconds.
	DURATION

	// like STRING, but 64-bit offsets. not yet implemented
	LARGE_STRING

	// like BINARY but with 64-bit offsets, not yet implemented
	LARGE_BINARY

	// like LIST but with 64-bit offsets. not yet implmented
	LARGE_LIST

	// calendar interval with three fields
	INTERVAL_MONTH_DAY_NANO

	// INTERVAL could be any of the interval types, kept to avoid breaking anyone
	// after switching to individual type ids for the interval types that were using
	// it when calling MakeFromData or NewBuilder
	//
	// Deprecated and will be removed in the next major version release
	INTERVAL

	// Alias to ensure we do not break any consumers
	DECIMAL = DECIMAL128
)

// DataType is the representation of an Arrow type.
type DataType interface {
	ID() Type
	// Name is name of the data type.
	Name() string
	Fingerprint() string
}

// FixedWidthDataType is the representation of an Arrow type that
// requires a fixed number of bits in memory for each element.
type FixedWidthDataType interface {
	DataType
	// BitWidth returns the number of bits required to store a single element of this data type in memory.
	BitWidth() int
}

type BinaryDataType interface {
	DataType
	binary()
}

func HashType(seed maphash.Seed, dt DataType) uint64 {
	var h maphash.Hash
	h.SetSeed(seed)
	h.WriteString(dt.Fingerprint())
	return h.Sum64()
}

func typeIDFingerprint(id Type) string {
	c := string(rune(int(id) + int('A')))
	return "@" + c
}

func typeFingerprint(typ DataType) string { return typeIDFingerprint(typ.ID()) }

func timeUnitFingerprint(unit TimeUnit) rune {
	switch unit {
	case Second:
		return 's'
	case Millisecond:
		return 'm'
	case Microsecond:
		return 'u'
	case Nanosecond:
		return 'n'
	default:
		debug.Assert(false, "unexpected time unit")
		return rune(0)
	}
}
