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

package util

import (
	"fmt"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/arrow/go/v16/arrow/util/util_message"
	"github.com/huandu/xstrings"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
	"strings"
	"testing"
)

func SetupTest() util_message.AllTheTypes {
	msg := util_message.ExampleMessage{
		Field1: "Example",
	}

	anyMsg, _ := anypb.New(&msg)

	return util_message.AllTheTypes{
		String_:     "Hello",
		Int32:       10,
		Int64:       100,
		Sint32:      -10,
		Sin64:       -100,
		Uint32:      10,
		Uint64:      100,
		Fixed32:     10,
		Fixed64:     1000,
		Sfixed32:    10,
		Bool:        false,
		Bytes:       []byte("Hello, world!"),
		Double:      1.1,
		Enum:        util_message.AllTheTypes_OPTION_0,
		Message:     &msg,
		Oneof:       &util_message.AllTheTypes_Oneofstring{Oneofstring: "World"},
		Any:         anyMsg,
		SimpleMap:   map[int32]string{99: "Hello", 100: "World"},
		ComplexMap:  map[string]*util_message.ExampleMessage{"complex": &msg},
		SimpleList:  []string{"Hello", "World"},
		ComplexList: []*util_message.ExampleMessage{&msg},
	}
}

func TestGetSchema(t *testing.T) {
	msg := SetupTest()

	pr := NewProtobufStructReflection(&msg)
	got := pr.GetSchema().String()
	want := `schema:
  fields: 22
    - string: type=utf8, nullable
    - int32: type=int32, nullable
    - int64: type=int64, nullable
    - sint32: type=int32, nullable
    - sin64: type=int64, nullable
    - uint32: type=uint32, nullable
    - uint64: type=uint64, nullable
    - fixed32: type=uint32, nullable
    - fixed64: type=uint64, nullable
    - sfixed32: type=int32, nullable
    - bool: type=bool, nullable
    - bytes: type=binary, nullable
    - double: type=float64, nullable
    - enum: type=dictionary<values=utf8, indices=int32, ordered=false>, nullable
    - message: type=struct<field1: utf8>, nullable
    - oneofstring: type=utf8, nullable
    - oneofmessage: type=struct<field1: utf8>, nullable
    - any: type=struct<field1: utf8>, nullable
    - simple_map: type=map<int32, utf8, items_nullable>, nullable
    - complex_map: type=map<utf8, struct<field1: utf8>, items_nullable>, nullable
    - simple_list: type=list<item: utf8, nullable>, nullable
    - complex_list: type=list<item: struct<field1: utf8>, nullable>, nullable`

	require.Equal(t, want, got)

	got = NewProtobufStructReflection(&msg, WithOneOfHandler(DenseUnion)).GetSchema().String()
	want = `schema:
  fields: 21
    - string: type=utf8, nullable
    - int32: type=int32, nullable
    - int64: type=int64, nullable
    - sint32: type=int32, nullable
    - sin64: type=int64, nullable
    - uint32: type=uint32, nullable
    - uint64: type=uint64, nullable
    - fixed32: type=uint32, nullable
    - fixed64: type=uint64, nullable
    - sfixed32: type=int32, nullable
    - bool: type=bool, nullable
    - bytes: type=binary, nullable
    - double: type=float64, nullable
    - enum: type=dictionary<values=utf8, indices=int32, ordered=false>, nullable
    - message: type=struct<field1: utf8>, nullable
    - oneof: type=dense_union<oneofstring: type=utf8, nullable=0, oneofmessage: type=struct<field1: utf8>, nullable=1>, nullable
    - any: type=struct<field1: utf8>, nullable
    - simple_map: type=map<int32, utf8, items_nullable>, nullable
    - complex_map: type=map<utf8, struct<field1: utf8>, items_nullable>, nullable
    - simple_list: type=list<item: utf8, nullable>, nullable
    - complex_list: type=list<item: struct<field1: utf8>, nullable>, nullable`

	require.Equal(t, want, got)

	excludeComplex := func(pfr protobufFieldReflection) bool {
		return pfr.isMap() || pfr.isList() || pfr.isStruct()
	}

	got = NewProtobufStructReflection(&msg, WithExclusionPolicy(excludeComplex)).GetSchema().String()
	want = `schema:
  fields: 15
    - string: type=utf8, nullable
    - int32: type=int32, nullable
    - int64: type=int64, nullable
    - sint32: type=int32, nullable
    - sin64: type=int64, nullable
    - uint32: type=uint32, nullable
    - uint64: type=uint64, nullable
    - fixed32: type=uint32, nullable
    - fixed64: type=uint64, nullable
    - sfixed32: type=int32, nullable
    - bool: type=bool, nullable
    - bytes: type=binary, nullable
    - double: type=float64, nullable
    - enum: type=dictionary<values=utf8, indices=int32, ordered=false>, nullable
    - oneofstring: type=utf8, nullable`

	require.Equal(t, want, got)

	got = NewProtobufStructReflection(
		&msg,
		WithExclusionPolicy(excludeComplex),
		WithFieldNameFormatter(xstrings.ToCamelCase),
	).GetSchema().String()
	want = `schema:
  fields: 15
    - String: type=utf8, nullable
    - Int32: type=int32, nullable
    - Int64: type=int64, nullable
    - Sint32: type=int32, nullable
    - Sin64: type=int64, nullable
    - Uint32: type=uint32, nullable
    - Uint64: type=uint64, nullable
    - Fixed32: type=uint32, nullable
    - Fixed64: type=uint64, nullable
    - Sfixed32: type=int32, nullable
    - Bool: type=bool, nullable
    - Bytes: type=binary, nullable
    - Double: type=float64, nullable
    - Enum: type=dictionary<values=utf8, indices=int32, ordered=false>, nullable
    - Oneofstring: type=utf8, nullable`

	require.Equal(t, want, got)
}

var (
	F32 arrow.UnionTypeCode = 7
	I32 arrow.UnionTypeCode = 13
)

func TestDenseUnion(t *testing.T) {
	dut := arrow.DenseUnionOf(
		[]arrow.Field{
			{Name: "f32", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
			{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		},
		[]arrow.UnionTypeCode{F32, I32},
	)

	childFloat32Bldr := array.NewFloat32Builder(memory.DefaultAllocator)
	childInt32Bldr := array.NewInt32Builder(memory.DefaultAllocator)
	ub := array.NewDenseUnionBuilder(memory.DefaultAllocator, dut)
	ub.Append(F32)
	ub.Child(0).(*array.Float32Builder).Append(6.9)

	defer ub.Release()
	defer childFloat32Bldr.Release()
	defer childInt32Bldr.Release()

	arr := ub.NewDenseUnionArray()
	defer arr.Release()

	var arrays []arrow.Array

	arrays = append(arrays, arr)

	structArray, _ := array.NewStructArray(arrays, []string{"dense_union_example"})

	schema := arrow.NewSchema([]arrow.Field{{
		Name:     "dense_union_example",
		Type:     dut,
		Nullable: true,
	}}, nil)

	record := array.RecordFromStructArray(structArray, schema)

	fmt.Println(record)

	jb, _ := record.MarshalJSON()

	fmt.Println(string(jb))
}

func TestRecordFromProtobuf2(t *testing.T) {
	msg := SetupTest()

	sm := NewSuperMessage(&msg, WithOneOfHandler(DenseUnion))

	fmt.Printf("%+v\n", sm)

	schema := sm.Schema()

	fmt.Println(schema)

	got := sm.Record(nil)

	fmt.Printf("%+v\n", got)

	jsonStr := `[
		{
			"string":"Hello",
			"int32":10,
			"int64":100,
			"sint32":-10,
			"sin64":-100,
			"uint32":10,
			"uint64":100,
			"fixed32":10,
			"fixed64":1000,
			"sfixed32":10,
			"bool":false,
			"bytes":"SGVsbG8sIHdvcmxkIQ==",
			"double":1.1,
			"enum":"OPTION_0",
			"message":{"field1":"Example"},
			"oneof": [0, "World"],
			"any":{"field1":"Example"},
			"simple_map":[{"key":99,"value":"Hello"},{"key":100,"value":"World"}],
			"complex_map":[{"key":"complex","value":{"field1":"Example"}}],
			"simple_list":["Hello","World"],
			"complex_list":[{"field1":"Example"}]
		}
	]`
	want, _, err := array.RecordFromJSON(memory.NewGoAllocator(), schema, strings.NewReader(jsonStr))

	require.NoError(t, err)
	require.True(t, array.RecordEqual(got, want))
}

func TestRecordFromProtobuf(t *testing.T) {
	msg := SetupTest()
	psr := NewProtobufStructReflection(&msg)
	schema := psr.GetSchema()
	got := RecordFromProtobuf(*psr, schema, nil)
	jsonStr := `[
		{
            "string":"Hello",
            "int32":10,
            "int64":100,
            "sint32":-10,
            "sin64":-100,
            "uint32":10,
            "uint64":100,
            "fixed32":10,
            "fixed64":1000,
            "sfixed32":10,
			"bool":false,
		    "bytes":"SGVsbG8sIHdvcmxkIQ==",
            "double":1.1,
            "enum":"OPTION_0",
            "message":{"field1":"Example"},
            "oneofstring":"World",
			"oneofmessage":{"field1":""},
			"any":{"field1":"Example"},
            "simple_map":[{"key":99,"value":"Hello"},{"key":100,"value":"World"}],
            "complex_map":[{"key":"complex","value":{"field1":"Example"}}],
            "simple_list":["Hello","World"],
            "complex_list":[{"field1":"Example"}]
		}
	]`
	want, _, err := array.RecordFromJSON(memory.NewGoAllocator(), schema, strings.NewReader(jsonStr))

	require.NoError(t, err)
	require.True(t, array.RecordEqual(got, want))
}
