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

package bmi_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v14/parquet/internal/bmi"
	"github.com/stretchr/testify/assert"
)

// Testing the issue in GH-37712
func TestBasicExtractBits(t *testing.T) {
	tests := []struct {
		bitmap, selection uint64
		expected          uint64
	}{
		{0, 0, 0},
		{0xFF, 0, 0},
		{0xFF, ^uint64(0), 0xFF},
		{0xFF00FF, 0xAAAA, 0x000F},
		{0xFF0AFF, 0xAFAA, 0x00AF},
		{0xFFAAFF, 0xAFAA, 0x03AF},
		{0xFECBDA9876543210, 0xF00FF00FF00FF00F, 0xFBD87430},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d-%d=>%d", tt.bitmap, tt.selection, tt.expected), func(t *testing.T) {
			assert.Equal(t, tt.expected, bmi.ExtractBits(tt.bitmap, tt.selection))
		})
	}
}
