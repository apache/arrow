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

package parquet

import (
	"encoding/binary"
	"reflect"
	"time"
	"unsafe"
)

const (
	julianUnixEpoch int64 = 2440588
	nanosPerDay     int64 = 3600 * 24 * 1000 * 1000 * 1000
	// Int96SizeBytes is the number of bytes that make up an Int96
	Int96SizeBytes int = 12
)

var (
	// Int96Traits provides information about the Int96 type
	Int96Traits int96Traits
	// ByteArrayTraits provides information about the ByteArray type, which is just an []byte
	ByteArrayTraits byteArrayTraits
	// FixedLenByteArrayTraits provides information about the FixedLenByteArray type which is just an []byte
	FixedLenByteArrayTraits fixedLenByteArrayTraits
	// ByteArraySizeBytes is the number of bytes returned by reflect.TypeOf(ByteArray{}).Size()
	ByteArraySizeBytes int = int(reflect.TypeOf(ByteArray{}).Size())
	// FixedLenByteArraySizeBytes is the number of bytes returned by reflect.TypeOf(FixedLenByteArray{}).Size()
	FixedLenByteArraySizeBytes int = int(reflect.TypeOf(FixedLenByteArray{}).Size())
)

// NewInt96 creates a new Int96 from the given 3 uint32 values.
func NewInt96(v [3]uint32) (out Int96) {
	binary.LittleEndian.PutUint32(out[0:], v[0])
	binary.LittleEndian.PutUint32(out[4:], v[1])
	binary.LittleEndian.PutUint32(out[8:], v[2])
	return
}

// Int96 is a 12 byte integer value utilized for representing timestamps as a 64 bit integer and a 32 bit
// integer.
type Int96 [12]byte

// SetNanoSeconds sets the Nanosecond field of the Int96 timestamp to the provided value
func (i96 *Int96) SetNanoSeconds(nanos int64) {
	binary.LittleEndian.PutUint64(i96[:8], uint64(nanos))
}

// String provides the string representation as a timestamp via converting to a time.Time
// and then calling String
func (i96 Int96) String() string {
	return i96.ToTime().String()
}

// ToTime returns a go time.Time object that represents the same time instant as the given Int96 value
func (i96 Int96) ToTime() time.Time {
	nanos := binary.LittleEndian.Uint64(i96[:8])
	jdays := binary.LittleEndian.Uint32(i96[8:])

	nanos = (uint64(jdays)-uint64(julianUnixEpoch))*uint64(nanosPerDay) + nanos
	t := time.Unix(0, int64(nanos))
	return t.UTC()
}

type int96Traits struct{}

func (int96Traits) BytesRequired(n int) int { return Int96SizeBytes * n }

func (int96Traits) CastFromBytes(b []byte) []Int96 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []Int96
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Int96SizeBytes
	s.Cap = h.Cap / Int96SizeBytes

	return res
}

func (int96Traits) CastToBytes(b []Int96) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Int96SizeBytes
	s.Cap = h.Cap * Int96SizeBytes

	return res
}

// ByteArray is a type to be utilized for representing the Parquet ByteArray physical type, represented as a byte slice
type ByteArray []byte

// Len returns the current length of the ByteArray, equivalent to len(bytearray)
func (b ByteArray) Len() int {
	return len(b)
}

// String returns a string representation of the ByteArray
func (b ByteArray) String() string {
	return *(*string)(unsafe.Pointer(&b))
}

type byteArrayTraits struct{}

func (byteArrayTraits) BytesRequired(n int) int {
	return ByteArraySizeBytes * n
}

func (byteArrayTraits) CastFromBytes(b []byte) []ByteArray {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []ByteArray
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / ByteArraySizeBytes
	s.Cap = h.Cap / ByteArraySizeBytes

	return res
}

// FixedLenByteArray is a go type to represent a FixedLengthByteArray as a byte slice
type FixedLenByteArray []byte

// Len returns the current length of this FixedLengthByteArray, equivalent to len(fixedlenbytearray)
func (b FixedLenByteArray) Len() int {
	return len(b)
}

// String returns a string representation of the FixedLenByteArray
func (b FixedLenByteArray) String() string {
	return *(*string)(unsafe.Pointer(&b))
}

type fixedLenByteArrayTraits struct{}

func (fixedLenByteArrayTraits) BytesRequired(n int) int {
	return FixedLenByteArraySizeBytes * n
}

func (fixedLenByteArrayTraits) CastFromBytes(b []byte) []FixedLenByteArray {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []FixedLenByteArray
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / FixedLenByteArraySizeBytes
	s.Cap = h.Cap / FixedLenByteArraySizeBytes

	return res
}
