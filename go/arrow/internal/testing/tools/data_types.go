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

//go:build go1.18

package tools

import (
	"reflect"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/float16"
	"golang.org/x/exp/constraints"
)

var typMap = map[reflect.Type]arrow.DataType{
	reflect.TypeOf(false):           arrow.FixedWidthTypes.Boolean,
	reflect.TypeOf(int8(0)):         arrow.PrimitiveTypes.Int8,
	reflect.TypeOf(int16(0)):        arrow.PrimitiveTypes.Int16,
	reflect.TypeOf(int32(0)):        arrow.PrimitiveTypes.Int32,
	reflect.TypeOf(int64(0)):        arrow.PrimitiveTypes.Int64,
	reflect.TypeOf(uint8(0)):        arrow.PrimitiveTypes.Uint8,
	reflect.TypeOf(uint16(0)):       arrow.PrimitiveTypes.Uint16,
	reflect.TypeOf(uint32(0)):       arrow.PrimitiveTypes.Uint32,
	reflect.TypeOf(uint64(0)):       arrow.PrimitiveTypes.Uint64,
	reflect.TypeOf(float32(0)):      arrow.PrimitiveTypes.Float32,
	reflect.TypeOf(float64(0)):      arrow.PrimitiveTypes.Float64,
	reflect.TypeOf(string("")):      arrow.BinaryTypes.String,
	reflect.TypeOf(arrow.Date32(0)): arrow.FixedWidthTypes.Date32,
	reflect.TypeOf(arrow.Date64(0)): arrow.FixedWidthTypes.Date64,
	reflect.TypeOf(true):            arrow.FixedWidthTypes.Boolean,
	reflect.TypeOf(float16.Num{}):   arrow.FixedWidthTypes.Float16,
	reflect.TypeOf([]byte{}):        arrow.BinaryTypes.Binary,
}

func GetDataType[T constraints.Integer | constraints.Float | bool | string | []byte | float16.Num]() arrow.DataType {
	var z T
	return typMap[reflect.TypeOf(z)]
}
