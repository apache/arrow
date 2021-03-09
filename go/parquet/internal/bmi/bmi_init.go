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

import (
	"golang.org/x/sys/cpu"
)

type funcs struct {
	extractBits func(uint64, uint64) uint64
	popcount64  func(uint64) uint64
	popcount32  func(uint32) uint32
	gtbitmap    func([]int16, int16) uint64
}

var funclist funcs

func init() {
	if cpu.X86.HasPOPCNT {
		funclist.popcount64 = popCount64BMI2
		funclist.popcount32 = popCount32BMI2
	} else {
		funclist.popcount64 = popCount64Go
		funclist.popcount32 = popCount32Go
	}
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

func ExtractBits(bitmap, selectBitmap uint64) uint64 {
	return funclist.extractBits(bitmap, selectBitmap)
}

func PopCount64(bitmap uint64) uint64 {
	return funclist.popcount64(bitmap)
}

func PopCount32(bitmap uint32) uint32 {
	return funclist.popcount32(bitmap)
}

func GreaterThanBitmap(levels []int16, rhs int16) uint64 {
	return funclist.gtbitmap(levels, rhs)
}
