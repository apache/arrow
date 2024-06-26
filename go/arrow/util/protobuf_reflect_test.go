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
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/arrow/util/util_message"
	"github.com/huandu/xstrings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
)

func SetupTest() util_message.AllTheTypes {
	msg := util_message.ExampleMessage{
		Field1: "Example",
	}

	anyMsg, _ := anypb.New(&msg)

	return util_message.AllTheTypes{
		Str:      "Hello",
		Int32:    10,
		Int64:    100,
		Sint32:   -10,
		Sin64:    -100,
		Uint32:   10,
		Uint64:   100,
		Fixed32:  10,
		Fixed64:  1000,
		Sfixed32: 10,
		Bool:     false,
		Bytes:    []byte("Hello, world!"),
		Double:   1.1,
		Enum:     util_message.AllTheTypes_OPTION_1,
		Message:  &msg,
		Oneof:    &util_message.AllTheTypes_Oneofstring{Oneofstring: "World"},
		Any:      anyMsg,
		//Breaks the test as the Golang maps have a non-deterministic order
		//SimpleMap:   map[int32]string{99: "Hello", 100: "World", 98: "How", 101: "Are", 1: "You"},
		SimpleMap:   map[int32]string{99: "Hello"},
		ComplexMap:  map[string]*util_message.ExampleMessage{"complex": &msg},
		SimpleList:  []string{"Hello", "World"},
		ComplexList: []*util_message.ExampleMessage{&msg},
	}
}

func TestGetSchema(t *testing.T) {
	msg := SetupTest()

	got := NewProtobufMessageReflection(&msg).Schema().String()
	want := `schema:
  fields: 22
    - str: type=utf8, nullable
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

	require.Equal(t, want, got, "got: %s\nwant: %s", got, want)

	got = NewProtobufMessageReflection(&msg, WithOneOfHandler(OneOfDenseUnion)).Schema().String()
	want = `schema:
  fields: 21
    - str: type=utf8, nullable
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

	require.Equal(t, want, got, "got: %s\nwant: %s", got, want)

	excludeComplex := func(pfr *ProtobufFieldReflection) bool {
		return pfr.isMap() || pfr.isList() || pfr.isStruct()
	}

	got = NewProtobufMessageReflection(&msg, WithExclusionPolicy(excludeComplex)).Schema().String()
	want = `schema:
  fields: 15
    - str: type=utf8, nullable
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

	require.Equal(t, want, got, "got: %s\nwant: %s", got, want)

	got = NewProtobufMessageReflection(
		&msg,
		WithExclusionPolicy(excludeComplex),
		WithFieldNameFormatter(xstrings.ToCamelCase),
	).Schema().String()
	want = `schema:
  fields: 15
    - Str: type=utf8, nullable
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

	require.Equal(t, want, got, "got: %s\nwant: %s", got, want)

	onlyEnum := func(pfr *ProtobufFieldReflection) bool {
		return !pfr.isEnum()
	}
	got = NewProtobufMessageReflection(
		&msg,
		WithExclusionPolicy(onlyEnum),
		WithEnumHandler(EnumNumber),
	).Schema().String()
	want = `schema:
  fields: 1
    - enum: type=int32, nullable`

	require.Equal(t, want, got, "got: %s\nwant: %s", got, want)

	got = NewProtobufMessageReflection(
		&msg,
		WithExclusionPolicy(onlyEnum),
		WithEnumHandler(EnumValue),
	).Schema().String()
	want = `schema:
  fields: 1
    - enum: type=utf8, nullable`

	require.Equal(t, want, got, "got: %s\nwant: %s", got, want)
}

func TestRecordFromProtobuf(t *testing.T) {
	msg := SetupTest()

	pmr := NewProtobufMessageReflection(&msg, WithOneOfHandler(OneOfDenseUnion))
	schema := pmr.Schema()
	got := pmr.Record(nil)
	jsonStr := `[
		{
			"str":"Hello",
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
			"enum":"OPTION_1",
			"message":{"field1":"Example"},
			"oneof": [0, "World"],
			"any":{"field1":"Example"},
			"simple_map":[{"key":99,"value":"Hello"}],
			"complex_map":[{"key":"complex","value":{"field1":"Example"}}],
			"simple_list":["Hello","World"],
			"complex_list":[{"field1":"Example"}]
		}
	]`
	want, _, err := array.RecordFromJSON(memory.NewGoAllocator(), schema, strings.NewReader(jsonStr))

	require.NoError(t, err)
	require.EqualExportedValues(t, got, want, "got: %s\nwant: %s", got, want)

	onlyEnum := func(pfr *ProtobufFieldReflection) bool { return !pfr.isEnum() }
	pmr = NewProtobufMessageReflection(&msg, WithExclusionPolicy(onlyEnum), WithEnumHandler(EnumValue))
	got = pmr.Record(nil)
	jsonStr = `[ { "enum":"OPTION_1" } ]`
	want, _, err = array.RecordFromJSON(memory.NewGoAllocator(), pmr.Schema(), strings.NewReader(jsonStr))
	require.NoError(t, err)
	require.True(t, array.RecordEqual(got, want), "got: %s\nwant: %s", got, want)

	pmr = NewProtobufMessageReflection(&msg, WithExclusionPolicy(onlyEnum), WithEnumHandler(EnumNumber))
	got = pmr.Record(nil)
	jsonStr = `[ { "enum":"1" } ]`
	want, _, err = array.RecordFromJSON(memory.NewGoAllocator(), pmr.Schema(), strings.NewReader(jsonStr))
	require.NoError(t, err)
	require.True(t, array.RecordEqual(got, want), "got: %s\nwant: %s", got, want)
}

func TestNullRecordFromProtobuf(t *testing.T) {
	pmr := NewProtobufMessageReflection(&util_message.AllTheTypes{})
	schema := pmr.Schema()
	got := pmr.Record(nil)
	_, _ = got.MarshalJSON()
	jsonStr := `[
		{
			"str":"",
			"int32":0,
			"int64":0,
			"sint32":0,
			"sin64":0,
			"uint32":0,
			"uint64":0,
			"fixed32":0,
			"fixed64":0,
			"sfixed32":0,
			"bool":false,
			"bytes":"",
			"double":0,
			"enum":"OPTION_0",
			"message":null,
			"oneofmessage":{"field1":""},
			"oneofstring":"",
			"any":null,
			"simple_map":[],
			"complex_map":[],
			"simple_list":[],
			"complex_list":[]
		}
	]`

	want, _, err := array.RecordFromJSON(memory.NewGoAllocator(), schema, strings.NewReader(jsonStr))

	require.NoError(t, err)
	require.EqualExportedValues(t, got, want, "got: %s\nwant: %s", got, want)
}

type testProtobufReflection struct {
	ProtobufFieldReflection
}

func (tpr testProtobufReflection) isNull() bool {
	return false
}

func TestAppendValueOrNull(t *testing.T) {
	unsupportedField := arrow.Field{Name: "Test", Type: arrow.FixedWidthTypes.Time32s}
	schema := arrow.NewSchema([]arrow.Field{unsupportedField}, nil)
	mem := memory.NewGoAllocator()
	recordBuilder := array.NewRecordBuilder(mem, schema)
	pmfr := ProtobufMessageFieldReflection{
		protobufReflection: &testProtobufReflection{},
		Field:              arrow.Field{Name: "Test", Type: arrow.FixedWidthTypes.Time32s},
	}
	got := pmfr.AppendValueOrNull(recordBuilder.Field(0), mem)
	want := "not able to appendValueOrNull for type TIME32"
	assert.EqualErrorf(t, got, want, "Error is: %v, want: %v", got, want)
}
