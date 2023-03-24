// Code generated by transpose_ints_amd64.go.tmpl. DO NOT EDIT.

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

//go:build !noasm

package utils

import (
	"golang.org/x/sys/cpu"
)

var (
	TransposeInt8Int8   func([]int8, []int8, []int32)
	TransposeInt8Uint8  func([]int8, []uint8, []int32)
	TransposeInt8Int16  func([]int8, []int16, []int32)
	TransposeInt8Uint16 func([]int8, []uint16, []int32)
	TransposeInt8Int32  func([]int8, []int32, []int32)
	TransposeInt8Uint32 func([]int8, []uint32, []int32)
	TransposeInt8Int64  func([]int8, []int64, []int32)
	TransposeInt8Uint64 func([]int8, []uint64, []int32)

	TransposeUint8Int8   func([]uint8, []int8, []int32)
	TransposeUint8Uint8  func([]uint8, []uint8, []int32)
	TransposeUint8Int16  func([]uint8, []int16, []int32)
	TransposeUint8Uint16 func([]uint8, []uint16, []int32)
	TransposeUint8Int32  func([]uint8, []int32, []int32)
	TransposeUint8Uint32 func([]uint8, []uint32, []int32)
	TransposeUint8Int64  func([]uint8, []int64, []int32)
	TransposeUint8Uint64 func([]uint8, []uint64, []int32)

	TransposeInt16Int8   func([]int16, []int8, []int32)
	TransposeInt16Uint8  func([]int16, []uint8, []int32)
	TransposeInt16Int16  func([]int16, []int16, []int32)
	TransposeInt16Uint16 func([]int16, []uint16, []int32)
	TransposeInt16Int32  func([]int16, []int32, []int32)
	TransposeInt16Uint32 func([]int16, []uint32, []int32)
	TransposeInt16Int64  func([]int16, []int64, []int32)
	TransposeInt16Uint64 func([]int16, []uint64, []int32)

	TransposeUint16Int8   func([]uint16, []int8, []int32)
	TransposeUint16Uint8  func([]uint16, []uint8, []int32)
	TransposeUint16Int16  func([]uint16, []int16, []int32)
	TransposeUint16Uint16 func([]uint16, []uint16, []int32)
	TransposeUint16Int32  func([]uint16, []int32, []int32)
	TransposeUint16Uint32 func([]uint16, []uint32, []int32)
	TransposeUint16Int64  func([]uint16, []int64, []int32)
	TransposeUint16Uint64 func([]uint16, []uint64, []int32)

	TransposeInt32Int8   func([]int32, []int8, []int32)
	TransposeInt32Uint8  func([]int32, []uint8, []int32)
	TransposeInt32Int16  func([]int32, []int16, []int32)
	TransposeInt32Uint16 func([]int32, []uint16, []int32)
	TransposeInt32Int32  func([]int32, []int32, []int32)
	TransposeInt32Uint32 func([]int32, []uint32, []int32)
	TransposeInt32Int64  func([]int32, []int64, []int32)
	TransposeInt32Uint64 func([]int32, []uint64, []int32)

	TransposeUint32Int8   func([]uint32, []int8, []int32)
	TransposeUint32Uint8  func([]uint32, []uint8, []int32)
	TransposeUint32Int16  func([]uint32, []int16, []int32)
	TransposeUint32Uint16 func([]uint32, []uint16, []int32)
	TransposeUint32Int32  func([]uint32, []int32, []int32)
	TransposeUint32Uint32 func([]uint32, []uint32, []int32)
	TransposeUint32Int64  func([]uint32, []int64, []int32)
	TransposeUint32Uint64 func([]uint32, []uint64, []int32)

	TransposeInt64Int8   func([]int64, []int8, []int32)
	TransposeInt64Uint8  func([]int64, []uint8, []int32)
	TransposeInt64Int16  func([]int64, []int16, []int32)
	TransposeInt64Uint16 func([]int64, []uint16, []int32)
	TransposeInt64Int32  func([]int64, []int32, []int32)
	TransposeInt64Uint32 func([]int64, []uint32, []int32)
	TransposeInt64Int64  func([]int64, []int64, []int32)
	TransposeInt64Uint64 func([]int64, []uint64, []int32)

	TransposeUint64Int8   func([]uint64, []int8, []int32)
	TransposeUint64Uint8  func([]uint64, []uint8, []int32)
	TransposeUint64Int16  func([]uint64, []int16, []int32)
	TransposeUint64Uint16 func([]uint64, []uint16, []int32)
	TransposeUint64Int32  func([]uint64, []int32, []int32)
	TransposeUint64Uint32 func([]uint64, []uint32, []int32)
	TransposeUint64Int64  func([]uint64, []int64, []int32)
	TransposeUint64Uint64 func([]uint64, []uint64, []int32)
)

func init() {
	if cpu.X86.HasAVX2 {

		TransposeInt8Int8 = transposeInt8Int8avx2
		TransposeInt8Uint8 = transposeInt8Uint8avx2
		TransposeInt8Int16 = transposeInt8Int16avx2
		TransposeInt8Uint16 = transposeInt8Uint16avx2
		TransposeInt8Int32 = transposeInt8Int32avx2
		TransposeInt8Uint32 = transposeInt8Uint32avx2
		TransposeInt8Int64 = transposeInt8Int64avx2
		TransposeInt8Uint64 = transposeInt8Uint64avx2

		TransposeUint8Int8 = transposeUint8Int8avx2
		TransposeUint8Uint8 = transposeUint8Uint8avx2
		TransposeUint8Int16 = transposeUint8Int16avx2
		TransposeUint8Uint16 = transposeUint8Uint16avx2
		TransposeUint8Int32 = transposeUint8Int32avx2
		TransposeUint8Uint32 = transposeUint8Uint32avx2
		TransposeUint8Int64 = transposeUint8Int64avx2
		TransposeUint8Uint64 = transposeUint8Uint64avx2

		TransposeInt16Int8 = transposeInt16Int8avx2
		TransposeInt16Uint8 = transposeInt16Uint8avx2
		TransposeInt16Int16 = transposeInt16Int16avx2
		TransposeInt16Uint16 = transposeInt16Uint16avx2
		TransposeInt16Int32 = transposeInt16Int32avx2
		TransposeInt16Uint32 = transposeInt16Uint32avx2
		TransposeInt16Int64 = transposeInt16Int64avx2
		TransposeInt16Uint64 = transposeInt16Uint64avx2

		TransposeUint16Int8 = transposeUint16Int8avx2
		TransposeUint16Uint8 = transposeUint16Uint8avx2
		TransposeUint16Int16 = transposeUint16Int16avx2
		TransposeUint16Uint16 = transposeUint16Uint16avx2
		TransposeUint16Int32 = transposeUint16Int32avx2
		TransposeUint16Uint32 = transposeUint16Uint32avx2
		TransposeUint16Int64 = transposeUint16Int64avx2
		TransposeUint16Uint64 = transposeUint16Uint64avx2

		TransposeInt32Int8 = transposeInt32Int8avx2
		TransposeInt32Uint8 = transposeInt32Uint8avx2
		TransposeInt32Int16 = transposeInt32Int16avx2
		TransposeInt32Uint16 = transposeInt32Uint16avx2
		TransposeInt32Int32 = transposeInt32Int32avx2
		TransposeInt32Uint32 = transposeInt32Uint32avx2
		TransposeInt32Int64 = transposeInt32Int64avx2
		TransposeInt32Uint64 = transposeInt32Uint64avx2

		TransposeUint32Int8 = transposeUint32Int8avx2
		TransposeUint32Uint8 = transposeUint32Uint8avx2
		TransposeUint32Int16 = transposeUint32Int16avx2
		TransposeUint32Uint16 = transposeUint32Uint16avx2
		TransposeUint32Int32 = transposeUint32Int32avx2
		TransposeUint32Uint32 = transposeUint32Uint32avx2
		TransposeUint32Int64 = transposeUint32Int64avx2
		TransposeUint32Uint64 = transposeUint32Uint64avx2

		TransposeInt64Int8 = transposeInt64Int8avx2
		TransposeInt64Uint8 = transposeInt64Uint8avx2
		TransposeInt64Int16 = transposeInt64Int16avx2
		TransposeInt64Uint16 = transposeInt64Uint16avx2
		TransposeInt64Int32 = transposeInt64Int32avx2
		TransposeInt64Uint32 = transposeInt64Uint32avx2
		TransposeInt64Int64 = transposeInt64Int64avx2
		TransposeInt64Uint64 = transposeInt64Uint64avx2

		TransposeUint64Int8 = transposeUint64Int8avx2
		TransposeUint64Uint8 = transposeUint64Uint8avx2
		TransposeUint64Int16 = transposeUint64Int16avx2
		TransposeUint64Uint16 = transposeUint64Uint16avx2
		TransposeUint64Int32 = transposeUint64Int32avx2
		TransposeUint64Uint32 = transposeUint64Uint32avx2
		TransposeUint64Int64 = transposeUint64Int64avx2
		TransposeUint64Uint64 = transposeUint64Uint64avx2

	} else if cpu.X86.HasSSE42 {

		TransposeInt8Int8 = transposeInt8Int8sse4
		TransposeInt8Uint8 = transposeInt8Uint8sse4
		TransposeInt8Int16 = transposeInt8Int16sse4
		TransposeInt8Uint16 = transposeInt8Uint16sse4
		TransposeInt8Int32 = transposeInt8Int32sse4
		TransposeInt8Uint32 = transposeInt8Uint32sse4
		TransposeInt8Int64 = transposeInt8Int64sse4
		TransposeInt8Uint64 = transposeInt8Uint64sse4

		TransposeUint8Int8 = transposeUint8Int8sse4
		TransposeUint8Uint8 = transposeUint8Uint8sse4
		TransposeUint8Int16 = transposeUint8Int16sse4
		TransposeUint8Uint16 = transposeUint8Uint16sse4
		TransposeUint8Int32 = transposeUint8Int32sse4
		TransposeUint8Uint32 = transposeUint8Uint32sse4
		TransposeUint8Int64 = transposeUint8Int64sse4
		TransposeUint8Uint64 = transposeUint8Uint64sse4

		TransposeInt16Int8 = transposeInt16Int8sse4
		TransposeInt16Uint8 = transposeInt16Uint8sse4
		TransposeInt16Int16 = transposeInt16Int16sse4
		TransposeInt16Uint16 = transposeInt16Uint16sse4
		TransposeInt16Int32 = transposeInt16Int32sse4
		TransposeInt16Uint32 = transposeInt16Uint32sse4
		TransposeInt16Int64 = transposeInt16Int64sse4
		TransposeInt16Uint64 = transposeInt16Uint64sse4

		TransposeUint16Int8 = transposeUint16Int8sse4
		TransposeUint16Uint8 = transposeUint16Uint8sse4
		TransposeUint16Int16 = transposeUint16Int16sse4
		TransposeUint16Uint16 = transposeUint16Uint16sse4
		TransposeUint16Int32 = transposeUint16Int32sse4
		TransposeUint16Uint32 = transposeUint16Uint32sse4
		TransposeUint16Int64 = transposeUint16Int64sse4
		TransposeUint16Uint64 = transposeUint16Uint64sse4

		TransposeInt32Int8 = transposeInt32Int8sse4
		TransposeInt32Uint8 = transposeInt32Uint8sse4
		TransposeInt32Int16 = transposeInt32Int16sse4
		TransposeInt32Uint16 = transposeInt32Uint16sse4
		TransposeInt32Int32 = transposeInt32Int32sse4
		TransposeInt32Uint32 = transposeInt32Uint32sse4
		TransposeInt32Int64 = transposeInt32Int64sse4
		TransposeInt32Uint64 = transposeInt32Uint64sse4

		TransposeUint32Int8 = transposeUint32Int8sse4
		TransposeUint32Uint8 = transposeUint32Uint8sse4
		TransposeUint32Int16 = transposeUint32Int16sse4
		TransposeUint32Uint16 = transposeUint32Uint16sse4
		TransposeUint32Int32 = transposeUint32Int32sse4
		TransposeUint32Uint32 = transposeUint32Uint32sse4
		TransposeUint32Int64 = transposeUint32Int64sse4
		TransposeUint32Uint64 = transposeUint32Uint64sse4

		TransposeInt64Int8 = transposeInt64Int8sse4
		TransposeInt64Uint8 = transposeInt64Uint8sse4
		TransposeInt64Int16 = transposeInt64Int16sse4
		TransposeInt64Uint16 = transposeInt64Uint16sse4
		TransposeInt64Int32 = transposeInt64Int32sse4
		TransposeInt64Uint32 = transposeInt64Uint32sse4
		TransposeInt64Int64 = transposeInt64Int64sse4
		TransposeInt64Uint64 = transposeInt64Uint64sse4

		TransposeUint64Int8 = transposeUint64Int8sse4
		TransposeUint64Uint8 = transposeUint64Uint8sse4
		TransposeUint64Int16 = transposeUint64Int16sse4
		TransposeUint64Uint16 = transposeUint64Uint16sse4
		TransposeUint64Int32 = transposeUint64Int32sse4
		TransposeUint64Uint32 = transposeUint64Uint32sse4
		TransposeUint64Int64 = transposeUint64Int64sse4
		TransposeUint64Uint64 = transposeUint64Uint64sse4

	} else {

		TransposeInt8Int8 = transposeInt8Int8
		TransposeInt8Uint8 = transposeInt8Uint8
		TransposeInt8Int16 = transposeInt8Int16
		TransposeInt8Uint16 = transposeInt8Uint16
		TransposeInt8Int32 = transposeInt8Int32
		TransposeInt8Uint32 = transposeInt8Uint32
		TransposeInt8Int64 = transposeInt8Int64
		TransposeInt8Uint64 = transposeInt8Uint64

		TransposeUint8Int8 = transposeUint8Int8
		TransposeUint8Uint8 = transposeUint8Uint8
		TransposeUint8Int16 = transposeUint8Int16
		TransposeUint8Uint16 = transposeUint8Uint16
		TransposeUint8Int32 = transposeUint8Int32
		TransposeUint8Uint32 = transposeUint8Uint32
		TransposeUint8Int64 = transposeUint8Int64
		TransposeUint8Uint64 = transposeUint8Uint64

		TransposeInt16Int8 = transposeInt16Int8
		TransposeInt16Uint8 = transposeInt16Uint8
		TransposeInt16Int16 = transposeInt16Int16
		TransposeInt16Uint16 = transposeInt16Uint16
		TransposeInt16Int32 = transposeInt16Int32
		TransposeInt16Uint32 = transposeInt16Uint32
		TransposeInt16Int64 = transposeInt16Int64
		TransposeInt16Uint64 = transposeInt16Uint64

		TransposeUint16Int8 = transposeUint16Int8
		TransposeUint16Uint8 = transposeUint16Uint8
		TransposeUint16Int16 = transposeUint16Int16
		TransposeUint16Uint16 = transposeUint16Uint16
		TransposeUint16Int32 = transposeUint16Int32
		TransposeUint16Uint32 = transposeUint16Uint32
		TransposeUint16Int64 = transposeUint16Int64
		TransposeUint16Uint64 = transposeUint16Uint64

		TransposeInt32Int8 = transposeInt32Int8
		TransposeInt32Uint8 = transposeInt32Uint8
		TransposeInt32Int16 = transposeInt32Int16
		TransposeInt32Uint16 = transposeInt32Uint16
		TransposeInt32Int32 = transposeInt32Int32
		TransposeInt32Uint32 = transposeInt32Uint32
		TransposeInt32Int64 = transposeInt32Int64
		TransposeInt32Uint64 = transposeInt32Uint64

		TransposeUint32Int8 = transposeUint32Int8
		TransposeUint32Uint8 = transposeUint32Uint8
		TransposeUint32Int16 = transposeUint32Int16
		TransposeUint32Uint16 = transposeUint32Uint16
		TransposeUint32Int32 = transposeUint32Int32
		TransposeUint32Uint32 = transposeUint32Uint32
		TransposeUint32Int64 = transposeUint32Int64
		TransposeUint32Uint64 = transposeUint32Uint64

		TransposeInt64Int8 = transposeInt64Int8
		TransposeInt64Uint8 = transposeInt64Uint8
		TransposeInt64Int16 = transposeInt64Int16
		TransposeInt64Uint16 = transposeInt64Uint16
		TransposeInt64Int32 = transposeInt64Int32
		TransposeInt64Uint32 = transposeInt64Uint32
		TransposeInt64Int64 = transposeInt64Int64
		TransposeInt64Uint64 = transposeInt64Uint64

		TransposeUint64Int8 = transposeUint64Int8
		TransposeUint64Uint8 = transposeUint64Uint8
		TransposeUint64Int16 = transposeUint64Int16
		TransposeUint64Uint16 = transposeUint64Uint16
		TransposeUint64Int32 = transposeUint64Int32
		TransposeUint64Uint32 = transposeUint64Uint32
		TransposeUint64Int64 = transposeUint64Int64
		TransposeUint64Uint64 = transposeUint64Uint64

	}
}
