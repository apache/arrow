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

package bmi

import "unsafe"

//go:noescape
func _extract_bits(bitmap, selectBitmap uint64, res unsafe.Pointer)

func extractBitsBMI2(bitmap, selectBitmap uint64) uint64 {
	var (
		res uint64
	)

	_extract_bits(bitmap, selectBitmap, unsafe.Pointer(&res))
	return res
}

//go:noescape
func _popcount64(bitmap uint64, res unsafe.Pointer)

func popCount64BMI2(bitmap uint64) uint64 {
	var (
		res uint64
	)

	_popcount64(bitmap, unsafe.Pointer(&res))
	return res
}

//go:noescape
func _popcount32(bitmap uint32, res unsafe.Pointer)

func popCount32BMI2(bitmap uint32) uint32 {
	var (
		res uint32
	)

	_popcount32(bitmap, unsafe.Pointer(&res))
	return res
}

//go:noescape
func _levels_to_bitmap(levels unsafe.Pointer, numLevels int, rhs int16, res unsafe.Pointer)

func greaterThanBitmapBMI2(levels []int16, rhs int16) uint64 {
	if levels == nil || len(levels) == 0 {
		return 0
	}

	var (
		p1  = unsafe.Pointer(&levels[0])
		p2  = len(levels)
		p3  = rhs
		res uint64
	)

	_levels_to_bitmap(p1, p2, p3, unsafe.Pointer(&res))
	return res
}
