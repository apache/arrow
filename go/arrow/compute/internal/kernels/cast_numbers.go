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

package kernels

//go:generate go run ../../../_tools/tmpl -i -data=cast_numbers.tmpldata -d arch=avx2 cast_numbers_simd_amd64.go.tmpl=cast_numbers_avx2_amd64.go
//go:generate go run ../../../_tools/tmpl -i -data=cast_numbers.tmpldata -d arch=sse4 cast_numbers_simd_amd64.go.tmpl=cast_numbers_sse4_amd64.go
//go:generate go run ../../../_tools/tmpl -i -data=cast_numbers.tmpldata cast_numbers_noasm.go.tmpl=cast_numbers_noasm.go
//go:generate go run ../../../_tools/tmpl -i -data=cast_numbers.tmpldata cast_numbers_amd64.go.tmpl=cast_numbers_amd64.go

func doStaticCast[InT, OutT numeric](in []InT, out []OutT) {
	for i, v := range in {
		out[i] = OutT(v)
	}
}

func castNumbersUnsafe(in, out any) {
	switch in := in.(type) {
	case []int8:
		switch out := out.(type) {
		case []int8:
			castInt8ToInt8(in, out)
		case []int16:
			castInt8ToInt16(in, out)
		case []int32:
			castInt8ToInt32(in, out)
		case []int64:
			castInt8ToInt64(in, out)
		case []uint8:
			castInt8ToUint8(in, out)
		case []uint16:
			castInt8ToUint16(in, out)
		case []uint32:
			castInt8ToUint32(in, out)
		case []uint64:
			castInt8ToUint64(in, out)
		case []float32:
			castInt8ToFloat32(in, out)
		case []float64:
			castInt8ToFloat64(in, out)
		}
	case []int16:
		switch out := out.(type) {
		case []int8:
			castInt16ToInt8(in, out)
		case []int16:
			castInt16ToInt16(in, out)
		case []int32:
			castInt16ToInt32(in, out)
		case []int64:
			castInt16ToInt64(in, out)
		case []uint8:
			castInt16ToUint8(in, out)
		case []uint16:
			castInt16ToUint16(in, out)
		case []uint32:
			castInt16ToUint32(in, out)
		case []uint64:
			castInt16ToUint64(in, out)
		case []float32:
			castInt16ToFloat32(in, out)
		case []float64:
			castInt16ToFloat64(in, out)
		}
	case []int32:
		switch out := out.(type) {
		case []int8:
			castInt32ToInt8(in, out)
		case []int16:
			castInt32ToInt16(in, out)
		case []int32:
			castInt32ToInt32(in, out)
		case []int64:
			castInt32ToInt64(in, out)
		case []uint8:
			castInt32ToUint8(in, out)
		case []uint16:
			castInt32ToUint16(in, out)
		case []uint32:
			castInt32ToUint32(in, out)
		case []uint64:
			castInt32ToUint64(in, out)
		case []float32:
			castInt32ToFloat32(in, out)
		case []float64:
			castInt32ToFloat64(in, out)
		}
	case []int64:
		switch out := out.(type) {
		case []int8:
			castInt64ToInt8(in, out)
		case []int16:
			castInt64ToInt16(in, out)
		case []int32:
			castInt64ToInt32(in, out)
		case []int64:
			castInt64ToInt64(in, out)
		case []uint8:
			castInt64ToUint8(in, out)
		case []uint16:
			castInt64ToUint16(in, out)
		case []uint32:
			castInt64ToUint32(in, out)
		case []uint64:
			castInt64ToUint64(in, out)
		case []float32:
			castInt64ToFloat32(in, out)
		case []float64:
			castInt64ToFloat64(in, out)
		}
	case []uint8:
		switch out := out.(type) {
		case []int8:
			castUint8ToInt8(in, out)
		case []int16:
			castUint8ToInt16(in, out)
		case []int32:
			castUint8ToInt32(in, out)
		case []int64:
			castUint8ToInt64(in, out)
		case []uint8:
			castUint8ToUint8(in, out)
		case []uint16:
			castUint8ToUint16(in, out)
		case []uint32:
			castUint8ToUint32(in, out)
		case []uint64:
			castUint8ToUint64(in, out)
		case []float32:
			castUint8ToFloat32(in, out)
		case []float64:
			castUint8ToFloat64(in, out)
		}
	case []uint16:
		switch out := out.(type) {
		case []int8:
			castUint16ToInt8(in, out)
		case []int16:
			castUint16ToInt16(in, out)
		case []int32:
			castUint16ToInt32(in, out)
		case []int64:
			castUint16ToInt64(in, out)
		case []uint8:
			castUint16ToUint8(in, out)
		case []uint16:
			castUint16ToUint16(in, out)
		case []uint32:
			castUint16ToUint32(in, out)
		case []uint64:
			castUint16ToUint64(in, out)
		case []float32:
			castUint16ToFloat32(in, out)
		case []float64:
			castUint16ToFloat64(in, out)
		}
	case []uint32:
		switch out := out.(type) {
		case []int8:
			castUint32ToInt8(in, out)
		case []int16:
			castUint32ToInt16(in, out)
		case []int32:
			castUint32ToInt32(in, out)
		case []int64:
			castUint32ToInt64(in, out)
		case []uint8:
			castUint32ToUint8(in, out)
		case []uint16:
			castUint32ToUint16(in, out)
		case []uint32:
			castUint32ToUint32(in, out)
		case []uint64:
			castUint32ToUint64(in, out)
		case []float32:
			castUint32ToFloat32(in, out)
		case []float64:
			castUint32ToFloat64(in, out)
		}
	case []uint64:
		switch out := out.(type) {
		case []int8:
			castUint64ToInt8(in, out)
		case []int16:
			castUint64ToInt16(in, out)
		case []int32:
			castUint64ToInt32(in, out)
		case []int64:
			castUint64ToInt64(in, out)
		case []uint8:
			castUint64ToUint8(in, out)
		case []uint16:
			castUint64ToUint16(in, out)
		case []uint32:
			castUint64ToUint32(in, out)
		case []uint64:
			castUint64ToUint64(in, out)
		case []float32:
			castUint64ToFloat32(in, out)
		case []float64:
			castUint64ToFloat64(in, out)
		}
	case []float32:
		switch out := out.(type) {
		case []int8:
			castFloat32ToInt8(in, out)
		case []int16:
			castFloat32ToInt16(in, out)
		case []int32:
			castFloat32ToInt32(in, out)
		case []int64:
			castFloat32ToInt64(in, out)
		case []uint8:
			castFloat32ToUint8(in, out)
		case []uint16:
			castFloat32ToUint16(in, out)
		case []uint32:
			castFloat32ToUint32(in, out)
		case []uint64:
			castFloat32ToUint64(in, out)
		case []float32:
			castFloat32ToFloat32(in, out)
		case []float64:
			castFloat32ToFloat64(in, out)
		}
	case []float64:
		switch out := out.(type) {
		case []int8:
			castFloat64ToInt8(in, out)
		case []int16:
			castFloat64ToInt16(in, out)
		case []int32:
			castFloat64ToInt32(in, out)
		case []int64:
			castFloat64ToInt64(in, out)
		case []uint8:
			castFloat64ToUint8(in, out)
		case []uint16:
			castFloat64ToUint16(in, out)
		case []uint32:
			castFloat64ToUint32(in, out)
		case []uint64:
			castFloat64ToUint64(in, out)
		case []float32:
			castFloat64ToFloat32(in, out)
		case []float64:
			castFloat64ToFloat64(in, out)
		}
	}
}
