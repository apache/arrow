// Code generated by type_simd_amd64.go.tmpl. DO NOT EDIT.

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

// +build !noasm

package math

import (
	"unsafe"

	"github.com/apache/arrow/go/arrow/array"
)

//go:noescape
func _sum_int64_avx2(buf unsafe.Pointer, len uintptr, res unsafe.Pointer)

func sum_int64_avx2(a *array.Int64) int64 {
	buf := a.Int64Values()
	var (
		p1  = unsafe.Pointer(&buf[0])
		p2  = uintptr(len(buf))
		res int64
	)
	_sum_int64_avx2(p1, p2, unsafe.Pointer(&res))
	return res
}
