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

package utils

import (
	"math"
)

// this file contains pure go implementations of the min_max functions that are
// SIMD accelerated so that we can fallback to these if the cpu doesn't support
// AVX2 or SSE4 instructions.

func int32MinMax(values []int32) (min, max int32) {
	min = math.MaxInt32
	max = math.MinInt32

	for _, v := range values {
		if min > v {
			min = v
		}
		if max < v {
			max = v
		}
	}
	return
}

func uint32MinMax(values []uint32) (min, max uint32) {
	min = math.MaxUint32
	max = 0

	for _, v := range values {
		if min > v {
			min = v
		}
		if max < v {
			max = v
		}
	}
	return
}

func int64MinMax(values []int64) (min, max int64) {
	min = math.MaxInt64
	max = math.MinInt64

	for _, v := range values {
		if min > v {
			min = v
		}
		if max < v {
			max = v
		}
	}
	return
}

func uint64MinMax(values []uint64) (min, max uint64) {
	min = math.MaxUint64
	max = 0

	for _, v := range values {
		if min > v {
			min = v
		}
		if max < v {
			max = v
		}
	}
	return
}

var minmaxFuncs = struct {
	i32  func([]int32) (int32, int32)
	ui32 func([]uint32) (uint32, uint32)
	i64  func([]int64) (int64, int64)
	ui64 func([]uint64) (uint64, uint64)
}{}

// GetMinMaxInt32 returns the min and max for a int32 slice, using AVX2 or
// SSE4 cpu extensions if available, falling back to a pure go implementation
// if they are unavailable or built with the noasm tag.
func GetMinMaxInt32(v []int32) (min, max int32) {
	return minmaxFuncs.i32(v)
}

// GetMinMaxUint32 returns the min and max for a uint32 slice, using AVX2 or
// SSE4 cpu extensions if available, falling back to a pure go implementation
// if they are unavailable or built with the noasm tag.
func GetMinMaxUint32(v []uint32) (min, max uint32) {
	return minmaxFuncs.ui32(v)
}

// GetMinMaxInt64 returns the min and max for a int64 slice, using AVX2 or
// SSE4 cpu extensions if available, falling back to a pure go implementation
// if they are unavailable or built with the noasm tag.
func GetMinMaxInt64(v []int64) (min, max int64) {
	return minmaxFuncs.i64(v)
}

// GetMinMaxUint64 returns the min and max for a uint64 slice, using AVX2 or
// SSE4 cpu extensions if available, falling back to a pure go implementation
// if they are unavailable or built with the noasm tag.
func GetMinMaxUint64(v []uint64) (min, max uint64) {
	return minmaxFuncs.ui64(v)
}
