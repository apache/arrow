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

package tools_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v12/arrow/internal/testing/tools"
	"github.com/stretchr/testify/assert"
)

func TestIntsToBitsLSB(t *testing.T) {
	tests := []struct {
		in  int32
		exp byte
	}{
		{0x11001010, 0x53},
		{0x00001111, 0xf0},
		{0x11110000, 0x0f},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%08x", test.in), func(t *testing.T) {
			got := tools.IntsToBitsLSB(test.in)
			assert.Equal(t, []byte{test.exp}, got)
		})
	}
}
