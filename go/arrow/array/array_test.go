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

package array_test

import (
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/internal/testing/tools"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/stretchr/testify/assert"
)

type testDataType struct {
	id arrow.Type
}

func (d *testDataType) ID() arrow.Type { return d.id }
func (d *testDataType) Name() string   { panic("implement me") }

func TestMakeFromData(t *testing.T) {
	tests := []struct {
		name     string
		d        arrow.DataType
		expPanic bool
		expError string
	}{
		// unsupported types
		{name: "null", d: &testDataType{arrow.NULL}, expPanic: true, expError: "unsupported data type: NULL"},
		{name: "map", d: &testDataType{arrow.MAP}, expPanic: true, expError: "unsupported data type: MAP"},

		// supported types
		{name: "bool", d: &testDataType{arrow.BOOL}},

		// invalid types
		{name: "invalid(-1)", d: &testDataType{arrow.Type(-1)}, expPanic: true, expError: "invalid data type: Type(-1)"},
		{name: "invalid(28)", d: &testDataType{arrow.Type(28)}, expPanic: true, expError: "invalid data type: Type(28)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var b [4]*memory.Buffer
			data := array.NewData(test.d, 0, b[:], 0)

			if test.expPanic {
				assert.PanicsWithValue(t, test.expError, func() {
					array.MakeFromData(data)
				})
			} else {
				assert.NotNil(t, array.MakeFromData(data))
			}
		})
	}
}

func bbits(v ...int32) []byte {
	return tools.IntsToBitsLSB(v...)
}

func TestArray_NullN(t *testing.T) {
	tests := []struct {
		name string
		l    int
		bm   []byte
		n    int
		exp  int
	}{
		{name: "unknown,l16", l: 16, bm: bbits(0x11001010, 0x00110011), n: array.UnknownNullCount, exp: 8},
		{name: "unknown,l12,ignores last nibble", l: 12, bm: bbits(0x11001010, 0x00111111), n: array.UnknownNullCount, exp: 6},
		{name: "unknown,l12,12 nulls", l: 12, bm: bbits(0x00000000, 0x00000000), n: array.UnknownNullCount, exp: 12},
		{name: "unknown,l12,00 nulls", l: 12, bm: bbits(0x11111111, 0x11111111), n: array.UnknownNullCount, exp: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buf := memory.NewBufferBytes(test.bm)
			data := array.NewData(arrow.FixedWidthTypes.Boolean, test.l, []*memory.Buffer{buf, nil}, test.n)
			buf.Release()
			ar := array.MakeFromData(data)
			data.Release()
			got := ar.NullN()
			ar.Release()
			assert.Equal(t, test.exp, got)
		})
	}
}
