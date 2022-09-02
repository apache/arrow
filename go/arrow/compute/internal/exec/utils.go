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

package exec

import (
	"fmt"
	"unsafe"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/decimal128"
	"github.com/apache/arrow/go/v10/arrow/decimal256"
	"github.com/apache/arrow/go/v10/arrow/float16"
	"golang.org/x/exp/constraints"
)

// IntTypes is a type constraint for raw values represented as signed
// integer types by Arrow. We aren't just using constraints.Signed
// because we don't want to include the raw `int` type here whose size
// changes based on the architecture (int32 on 32-bit architectures and
// int64 on 64-bit architectures).
//
// This will also cover types like MonthInterval or the time types
// as their underlying types are int32 and int64 which will get covered
// by using the ~
type IntTypes interface {
	~int8 | ~int16 | ~int32 | ~int64
}

// UintTypes is a type constraint for raw values represented as unsigned
// integer types by Arrow. We aren't just using constraints.Unsigned
// because we don't want to include the raw `uint` type here whose size
// changes based on the architecture (uint32 on 32-bit architectures and
// uint64 on 64-bit architectures). We also don't want to include uintptr
type UintTypes interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64
}

// FloatTypes is a type constraint for raw values for representing
// floating point values in Arrow. This consists of constraints.Float and
// float16.Num
type FloatTypes interface {
	float16.Num | constraints.Float
}

// DecimalTypes is a type constraint for raw values representing larger
// decimal type values in Arrow, specifically decimal128 and decimal256.
type DecimalTypes interface {
	decimal128.Num | decimal256.Num
}

// FixedWidthTypes is a type constraint for raw values in Arrow that
// can be represented as FixedWidth byte slices. Specifically this is for
// using Go generics to easily re-type a byte slice to a properly-typed
// slice. Booleans are excluded here since they are represented by Arrow
// as a bitmap and thus the buffer can't be just reinterpreted as a []bool
type FixedWidthTypes interface {
	IntTypes | UintTypes |
		FloatTypes | DecimalTypes |
		arrow.DayTimeInterval | arrow.MonthDayNanoInterval
}

type TemporalTypes interface {
	arrow.Date32 | arrow.Date64 | arrow.Time32 | arrow.Time64 |
		arrow.Timestamp | arrow.Duration | arrow.DayTimeInterval |
		arrow.MonthInterval | arrow.MonthDayNanoInterval
}

// GetSpanValues returns a properly typed slice bye reinterpreting
// the buffer at index i using unsafe.Slice. This will take into account
// the offset of the given ArraySpan.
func GetSpanValues[T FixedWidthTypes](span *ArraySpan, i int) []T {
	ret := unsafe.Slice((*T)(unsafe.Pointer(&span.Buffers[i].Buf[0])), span.Offset+span.Len)
	return ret[span.Offset:]
}

// GetSpanOffsets is like GetSpanValues, except it is only for int32
// or int64 and adds the additional 1 expected value for an offset
// buffer (ie. len(output) == span.Len+1)
func GetSpanOffsets[T int32 | int64](span *ArraySpan, i int) []T {
	ret := unsafe.Slice((*T)(unsafe.Pointer(&span.Buffers[i].Buf[0])), span.Offset+span.Len+1)
	return ret[span.Offset:]
}

func GetBytes[T constraints.Integer](in []T) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&in[0])), len(in)*int(unsafe.Sizeof(T(0))))
}

func Min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

// OptionsInit should be used in the case where a KernelState is simply
// represented with a specific type by value (instead of pointer).
// This will initialize the KernelState as a value-copied instance of
// the passed in function options argument to ensure separation
// and allow the kernel to manipulate the options if necessary without
// any negative consequences since it will have its own copy of the options.
func OptionsInit[T any](_ *KernelCtx, args KernelInitArgs) (KernelState, error) {
	if opts, ok := args.Options.(*T); ok {
		return *opts, nil
	}

	return nil, fmt.Errorf("%w: attempted to initialize kernel state from invalid function options",
		arrow.ErrInvalid)
}
