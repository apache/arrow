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
	"encoding/json"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/arrow/util/util_message"
	"github.com/huandu/xstrings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
)

type Fixture struct {
	msg     proto.Message
	schema  string
	jsonStr string
}

type J map[string]any

func AllTheTypesFixture() Fixture {
	e := J{"field1": "Example"}

	m := J{
		"str":          "Hello",
		"int32":        10,
		"int64":        100,
		"sint32":       -10,
		"sin64":        -100,
		"uint32":       10,
		"uint64":       100,
		"fixed32":      10,
		"fixed64":      1000,
		"sfixed32":     10,
		"bool":         false,
		"bytes":        "SGVsbG8sIHdvcmxkIQ==",
		"double":       1.1,
		"enum":         "OPTION_1",
		"message":      e,
		"oneof":        []any{0, "World"},
		"any":          J{"field1": "Example"},
		"simple_map":   []J{{"key": 99, "value": "Hello"}},
		"complex_map":  []J{{"key": "complex", "value": e}},
		"simple_list":  []any{"Hello", "World"},
		"complex_list": []J{e},
	}
	jm, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	jsonString := string(jm)

	exampleMsg := util_message.ExampleMessage{
		Field1: "Example",
	}
	anyMsg, _ := anypb.New(&exampleMsg)

	msg := util_message.AllTheTypes{
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
		Message:  &exampleMsg,
		Oneof:    &util_message.AllTheTypes_Oneofstring{Oneofstring: "World"},
		Any:      anyMsg,
		//Breaks the test as the Golang maps have a non-deterministic order
		//SimpleMap:   map[int32]string{99: "Hello", 100: "World", 98: "How", 101: "Are", 1: "You"},
		SimpleMap:   map[int32]string{99: "Hello"},
		ComplexMap:  map[string]*util_message.ExampleMessage{"complex": &exampleMsg},
		SimpleList:  []string{"Hello", "World"},
		ComplexList: []*util_message.ExampleMessage{&exampleMsg},
	}

	schema := `schema:
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

	return Fixture{
		msg:     &msg,
		schema:  schema,
		jsonStr: jsonString,
	}
}

func AllTheTypesNoAnyFixture() Fixture {
	exampleMsg := util_message.ExampleMessage{
		Field1: "Example",
	}

	msg := util_message.AllTheTypesNoAny{
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
		Enum:     util_message.AllTheTypesNoAny_OPTION_1,
		Message:  &exampleMsg,
		Oneof:    &util_message.AllTheTypesNoAny_Oneofstring{Oneofstring: "World"},
		//Breaks the test as the Golang maps have a non-deterministic order
		//SimpleMap:   map[int32]string{99: "Hello", 100: "World", 98: "How", 101: "Are", 1: "You"},
		SimpleMap:   map[int32]string{99: "Hello"},
		ComplexMap:  map[string]*util_message.ExampleMessage{"complex": &exampleMsg},
		SimpleList:  []string{"Hello", "World"},
		ComplexList: []*util_message.ExampleMessage{&exampleMsg},
	}

	schema := `schema:
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
    - simple_map: type=map<int32, utf8, items_nullable>, nullable
    - complex_map: type=map<utf8, struct<field1: utf8>, items_nullable>, nullable
    - simple_list: type=list<item: utf8, nullable>, nullable
    - complex_list: type=list<item: struct<field1: utf8>, nullable>, nullable`

	jsonStr := `{
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
			"oneofmessage": { "field1": null },
			"oneofstring": "World",
			"simple_map":[{"key":99,"value":"Hello"}],
			"complex_map":[{"key":"complex","value":{"field1":"Example"}}],
			"simple_list":["Hello","World"],
			"complex_list":[{"field1":"Example"}]
		}`

	return Fixture{
		msg:     &msg,
		schema:  schema,
		jsonStr: jsonStr,
	}
}

func CheckSchema(t *testing.T, pmr *ProtobufMessageReflection, want string) {
	got := pmr.Schema().String()
	require.Equal(t, got, want, "got: %s\nwant: %s", got, want)
}

func CheckRecord(t *testing.T, pmr *ProtobufMessageReflection, jsonStr string) {
	rec := pmr.Record(nil)
	got, err := json.Marshal(rec)
	assert.NoError(t, err)
	assert.JSONEq(t, jsonStr, string(got), "got: %s\nwant: %s", got, jsonStr)
}

func TestGetSchema(t *testing.T) {
	f := AllTheTypesFixture()

	pmr := NewProtobufMessageReflection(f.msg)
	CheckSchema(t, pmr, f.schema)

	pmr = NewProtobufMessageReflection(f.msg, WithOneOfHandler(OneOfDenseUnion))
	want := `schema:
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
	CheckSchema(t, pmr, want)

	excludeComplex := func(pfr *ProtobufFieldReflection) bool {
		return pfr.isMap() || pfr.isList() || pfr.isStruct()
	}

	pmr = NewProtobufMessageReflection(f.msg, WithExclusionPolicy(excludeComplex))
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
	CheckSchema(t, pmr, want)

	pmr = NewProtobufMessageReflection(
		f.msg,
		WithExclusionPolicy(excludeComplex),
		WithFieldNameFormatter(xstrings.ToCamelCase),
	)
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
	CheckSchema(t, pmr, want)

	onlyEnum := func(pfr *ProtobufFieldReflection) bool {
		return !pfr.isEnum()
	}
	pmr = NewProtobufMessageReflection(
		f.msg,
		WithExclusionPolicy(onlyEnum),
		WithEnumHandler(EnumNumber),
	)
	want = `schema:
  fields: 1
    - enum: type=int32, nullable`
	CheckSchema(t, pmr, want)

	pmr = NewProtobufMessageReflection(
		f.msg,
		WithExclusionPolicy(onlyEnum),
		WithEnumHandler(EnumValue),
	)
	want = `schema:
  fields: 1
    - enum: type=utf8, nullable`
	CheckSchema(t, pmr, want)
}

func TestRecordFromProtobuf(t *testing.T) {
	f := AllTheTypesFixture()

	pmr := NewProtobufMessageReflection(f.msg, WithOneOfHandler(OneOfDenseUnion))
	CheckRecord(t, pmr, fmt.Sprintf(`[%s]`, f.jsonStr))

	onlyEnum := func(pfr *ProtobufFieldReflection) bool { return !pfr.isEnum() }
	pmr = NewProtobufMessageReflection(f.msg, WithExclusionPolicy(onlyEnum), WithEnumHandler(EnumValue))
	jsonStr := `[ { "enum":"OPTION_1" } ]`
	CheckRecord(t, pmr, jsonStr)

	pmr = NewProtobufMessageReflection(f.msg, WithExclusionPolicy(onlyEnum), WithEnumHandler(EnumNumber))
	jsonStr = `[ { "enum":1 } ]`
	CheckRecord(t, pmr, jsonStr)
}

func TestNullRecordFromProtobuf(t *testing.T) {
	pmr := NewProtobufMessageReflection(&util_message.AllTheTypes{})
	CheckRecord(t, pmr, `[{
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
		"bytes":null,
		"double":0,
		"enum":"OPTION_0",
		"message":null,
		"oneofmessage":{"field1":""},
		"oneofstring":"",
		"any": null,
		"simple_map":[],
		"complex_map":[],
		"simple_list":[],
		"complex_list":[]
	}]`)
}

func TestExcludedNested(t *testing.T) {
	msg := util_message.ExampleMessage{
		Field1: "Example",
	}
	schema := `schema:
  fields: 2
    - simple_a: type=list<item: struct<field1: utf8>, nullable>, nullable
    - simple_b: type=list<item: struct<field1: utf8>, nullable>, nullable`

	simpleNested := util_message.SimpleNested{
		SimpleA: []*util_message.ExampleMessage{&msg},
		SimpleB: []*util_message.ExampleMessage{&msg},
	}
	pmr := NewProtobufMessageReflection(&simpleNested)
	jsonStr := `[{ "simple_a":[{"field1":"Example"}], "simple_b":[{"field1":"Example"}] }]`
	CheckSchema(t, pmr, schema)
	CheckRecord(t, pmr, jsonStr)

	//exclude one value
	simpleNested = util_message.SimpleNested{
		SimpleA: []*util_message.ExampleMessage{&msg},
	}
	jsonStr = `[{ "simple_a":[{"field1":"Example"}], "simple_b":[]}]`
	CheckSchema(t, pmr, schema)
	CheckRecord(t, pmr, jsonStr)

	////exclude both values
	simpleNested = util_message.SimpleNested{}
	jsonStr = `[{ "simple_a":[], "simple_b":[] }]`
	CheckSchema(t, pmr, schema)
	CheckRecord(t, pmr, jsonStr)

	f := AllTheTypesNoAnyFixture()
	schema = `schema:
  fields: 2
    - all_the_types_no_any_a: type=list<item: struct<str: utf8, int32: int32, int64: int64, sint32: int32, sin64: int64, uint32: uint32, uint64: uint64, fixed32: uint32, fixed64: uint64, sfixed32: int32, bool: bool, bytes: binary, double: float64, enum: dictionary<values=utf8, indices=int32, ordered=false>, message: struct<field1: utf8>, oneofstring: utf8, oneofmessage: struct<field1: utf8>, simple_map: map<int32, utf8, items_nullable>, complex_map: map<utf8, struct<field1: utf8>, items_nullable>, simple_list: list<item: utf8, nullable>, complex_list: list<item: struct<field1: utf8>, nullable>>, nullable>, nullable
    - all_the_types_no_any_b: type=list<item: struct<str: utf8, int32: int32, int64: int64, sint32: int32, sin64: int64, uint32: uint32, uint64: uint64, fixed32: uint32, fixed64: uint64, sfixed32: int32, bool: bool, bytes: binary, double: float64, enum: dictionary<values=utf8, indices=int32, ordered=false>, message: struct<field1: utf8>, oneofstring: utf8, oneofmessage: struct<field1: utf8>, simple_map: map<int32, utf8, items_nullable>, complex_map: map<utf8, struct<field1: utf8>, items_nullable>, simple_list: list<item: utf8, nullable>, complex_list: list<item: struct<field1: utf8>, nullable>>, nullable>, nullable`

	complexNested := util_message.ComplexNested{
		AllTheTypesNoAnyA: []*util_message.AllTheTypesNoAny{f.msg.(*util_message.AllTheTypesNoAny)},
		AllTheTypesNoAnyB: []*util_message.AllTheTypesNoAny{f.msg.(*util_message.AllTheTypesNoAny)},
	}
	jsonStr = fmt.Sprintf(`[{ "all_the_types_no_any_a": [%s], "all_the_types_no_any_b": [%s] }]`, f.jsonStr, f.jsonStr)
	pmr = NewProtobufMessageReflection(&complexNested)
	CheckSchema(t, pmr, schema)
	CheckRecord(t, pmr, jsonStr)

	// exclude one value
	complexNested = util_message.ComplexNested{
		AllTheTypesNoAnyB: []*util_message.AllTheTypesNoAny{f.msg.(*util_message.AllTheTypesNoAny)},
	}
	jsonStr = fmt.Sprintf(`[{ "all_the_types_no_any_a": [], "all_the_types_no_any_b": [%s] }]`, f.jsonStr)
	pmr = NewProtobufMessageReflection(&complexNested)
	CheckSchema(t, pmr, schema)
	CheckRecord(t, pmr, jsonStr)

	// exclude both values
	complexNested = util_message.ComplexNested{}
	jsonStr = `[{ "all_the_types_no_any_a": [], "all_the_types_no_any_b": [] }]`
	pmr = NewProtobufMessageReflection(&complexNested)
	CheckSchema(t, pmr, schema)
	CheckRecord(t, pmr, jsonStr)

	schema = `schema:
  fields: 2
    - complex_nested: type=struct<all_the_types_no_any_a: list<item: struct<str: utf8, int32: int32, int64: int64, sint32: int32, sin64: int64, uint32: uint32, uint64: uint64, fixed32: uint32, fixed64: uint64, sfixed32: int32, bool: bool, bytes: binary, double: float64, enum: dictionary<values=utf8, indices=int32, ordered=false>, message: struct<field1: utf8>, oneofstring: utf8, oneofmessage: struct<field1: utf8>, simple_map: map<int32, utf8, items_nullable>, complex_map: map<utf8, struct<field1: utf8>, items_nullable>, simple_list: list<item: utf8, nullable>, complex_list: list<item: struct<field1: utf8>, nullable>>, nullable>, all_the_types_no_any_b: list<item: struct<str: utf8, int32: int32, int64: int64, sint32: int32, sin64: int64, uint32: uint32, uint64: uint64, fixed32: uint32, fixed64: uint64, sfixed32: int32, bool: bool, bytes: binary, double: float64, enum: dictionary<values=utf8, indices=int32, ordered=false>, message: struct<field1: utf8>, oneofstring: utf8, oneofmessage: struct<field1: utf8>, simple_map: map<int32, utf8, items_nullable>, complex_map: map<utf8, struct<field1: utf8>, items_nullable>, simple_list: list<item: utf8, nullable>, complex_list: list<item: struct<field1: utf8>, nullable>>, nullable>>, nullable
    - simple_nested: type=struct<simple_a: list<item: struct<field1: utf8>, nullable>, simple_b: list<item: struct<field1: utf8>, nullable>>, nullable`

	deepNested := util_message.DeepNested{
		ComplexNested: &complexNested,
		SimpleNested:  &simpleNested,
	}
	jsonStr = `[{ "simple_nested": {"simple_a":[], "simple_b":[]}, "complex_nested": {"all_the_types_no_any_a": [], "all_the_types_no_any_b": []} }]`
	pmr = NewProtobufMessageReflection(&deepNested)
	CheckSchema(t, pmr, schema)
	CheckRecord(t, pmr, jsonStr)

	// exclude one value
	deepNested = util_message.DeepNested{
		ComplexNested: &complexNested,
	}
	jsonStr = `[{ "simple_nested": null, "complex_nested": {"all_the_types_no_any_a": [], "all_the_types_no_any_b": []} }]`
	pmr = NewProtobufMessageReflection(&deepNested)
	CheckSchema(t, pmr, schema)
	CheckRecord(t, pmr, jsonStr)

	// exclude both values
	deepNested = util_message.DeepNested{}
	pmr = NewProtobufMessageReflection(&deepNested)
	jsonStr = `[{ "simple_nested": null, "complex_nested": null }]`
	CheckSchema(t, pmr, schema)
	CheckRecord(t, pmr, jsonStr)
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
