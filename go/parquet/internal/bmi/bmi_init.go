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

// Package bmi contains helpers for manipulating bitmaps via BMI2 extensions
// properly falling back to pure go implementations if the CPU doesn't support
// BMI2.
package bmi

import (
	"golang.org/x/sys/cpu"
)

type funcs struct {
	extractBits func(uint64, uint64) uint64
	gtbitmap    func([]int16, int16) uint64
}

var funclist funcs

func init() {
	if cpu.X86.HasBMI2 {
		funclist.extractBits = extractBitsBMI2
	} else {
		funclist.extractBits = extractBitsGo
	}
	if cpu.X86.HasAVX2 {
		funclist.gtbitmap = greaterThanBitmapBMI2
	} else {
		funclist.gtbitmap = greaterThanBitmapGo
	}
}

// ExtractBits performs a Parallel Bit extract as per the PEXT instruction for
// x86/x86-64 cpus to use the second parameter as a mask to extract the bits from
// the first argument into a new bitmap.
//
// For each bit Set in selectBitmap, the corresponding bits are extracted from bitmap
// and written to contiguous lower bits of the result, the remaining upper bits are zeroed.
func ExtractBits(bitmap, selectBitmap uint64) uint64 {
	return funclist.extractBits(bitmap, selectBitmap)
}

// GreaterThanBitmap builds a bitmap where each bit corresponds to whether or not
// the level in that index is greater than the value of rhs.
func GreaterThanBitmap(levels []int16, rhs int16) uint64 {
	return funclist.gtbitmap(levels, rhs)
}
