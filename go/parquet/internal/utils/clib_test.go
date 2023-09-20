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
	"fmt"
	"testing"
	"unsafe"
)

func TestClibMemcpy(t *testing.T) {
	src := make([]byte, 256)
	zero := make([]byte, 256)
	dst := make([]byte, 256)

	for i := range src {
		src[i] = byte(i)
	}

	for count := 0; count < 256; count++ {
		copy(dst[:], zero[:])

		t.Run(fmt.Sprint(count), func(t *testing.T) {
			ptr := _ClibMemcpy(unsafe.Pointer(&dst[0]), unsafe.Pointer(&src[0]), uint(count))
			if unsafe.Pointer(&dst[0]) != ptr {
				t.Errorf("TestClibMemcpy(): \nexpected %v\ngot     %v", unsafe.Pointer(&dst[0]), ptr)
			}
			i := 0
			for ; i < count; i++ {
				if dst[i] != src[i] {
					t.Errorf("TestClibMemcpy(): \nexpected %d\ngot     %d", src[i], dst[i])
				}
			}
			for ; i < len(dst); i++ {
				if dst[i] != 0 {
					t.Errorf("TestClibMemcpy(): \nexpected %d\ngot     %d", 0, dst[i])
				}
			}
		})
	}
}

func TestClibMemset(t *testing.T) {
	init := make([]byte, 256)
	dst := make([]byte, 256)

	for i := range init {
		init[i] = byte(i)
	}

	for count := 0; count < 256; count++ {
		copy(dst[:], init[:])

		t.Run(fmt.Sprint(count), func(t *testing.T) {
			ptr := _ClibMemset(unsafe.Pointer(&dst[0]), count, uint(count))
			if unsafe.Pointer(&dst[0]) != ptr {
				t.Errorf("TestClibMemset(): \nexpected %v\ngot     %v", unsafe.Pointer(&dst[0]), ptr)
			}

			i := 0
			for ; i < count; i++ {
				if dst[i] != byte(count) {
					t.Errorf("1-TestClibMemset(%d): \nexpected %d\ngot     %d", i, count, dst[i])
				}
			}
			for ; i < len(dst); i++ {
				if dst[i] != init[i] {
					t.Errorf("2-TestClibMemset(%d): \nexpected %d\ngot     %d", i, init[i], dst[i])
				}
			}
		})
	}
}
