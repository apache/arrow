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

package kernels

import (
	"unsafe"

	"github.com/apache/arrow/go/v10/arrow"
)

//go:noescape
func _arithmetic_avx2(typ int, op int8, inLeft, inRight, out unsafe.Pointer, len int)

func arithmeticAvx2(typ arrow.Type, op ArithmeticOp, left, right, out []byte, len int) {
	_arithmetic_avx2(int(typ), int8(op), unsafe.Pointer(&left[0]), unsafe.Pointer(&right[0]), unsafe.Pointer(&out[0]), len)
}

//go:noescape
func _arithmetic_arr_scalar_avx2(typ int, op int8, inLeft, inRight, out unsafe.Pointer, len int)

func arithmeticArrScalarAvx2(typ arrow.Type, op ArithmeticOp, left []byte, right unsafe.Pointer, out []byte, len int) {
	_arithmetic_arr_scalar_avx2(int(typ), int8(op), unsafe.Pointer(&left[0]), right, unsafe.Pointer(&out[0]), len)
}

//go:noescape
func _arithmetic_scalar_arr_avx2(typ int, op int8, inLeft, inRight, out unsafe.Pointer, len int)

func arithmeticScalarArrAvx2(typ arrow.Type, op ArithmeticOp, left unsafe.Pointer, right, out []byte, len int) {
	_arithmetic_scalar_arr_avx2(int(typ), int8(op), left, unsafe.Pointer(&right[0]), unsafe.Pointer(&out[0]), len)
}
