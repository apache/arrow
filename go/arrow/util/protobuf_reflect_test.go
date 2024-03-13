package util

import (
	"github.com/apache/arrow/go/v16/arrow/util/util_message"
	"github.com/huandu/xstrings"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
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
		Enum:        0,
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
    - enum: type=int32, nullable
    - message: type=struct<field1: utf8>, nullable
    - oneofstring: type=utf8, nullable
    - oneofmessage: type=struct<field1: utf8>, nullable
    - any: type=struct<field1: utf8>, nullable
    - simple_map: type=map<int32, utf8, items_nullable>, nullable
    - complex_map: type=map<utf8, struct<field1: utf8>, items_nullable>, nullable
    - simple_list: type=list<item: utf8, nullable>, nullable
    - complex_list: type=list<item: struct<field1: utf8>, nullable>, nullable`

	require.Equal(t, want, got)

	excludeComplex := func(pfr ProtobufFieldReflection) bool {
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
    - enum: type=int32, nullable
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
    - Enum: type=int32, nullable
    - Oneofstring: type=utf8, nullable`

	require.Equal(t, want, got)
}

func TestRecordFromProtobuf(t *testing.T) {
	msg := SetupTest()

	psr := NewProtobufStructReflection(&msg)

	schema := psr.GetSchema()
	record := RecordFromProtobuf(*psr, schema)

	want := []byte(`[{"any":{"field1":"Example"},"bool":false,"bytes":"SGVsbG8sIHdvcmxkIQ==","complex_list":[{"field1":"Example"}],"complex_map":[{"key":"complex","value":{"field1":"Example"}}],"double":1.1,"enum":0,"fixed32":10,"fixed64":1000,"int32":10,"int64":100,"message":{"field1":"Example"},"oneofmessage":{"field1":""},"oneofstring":"World","sfixed32":10,"simple_list":["Hello","World"],"simple_map":[{"key":99,"value":"Hello"},{"key":100,"value":"World"}],"sin64":-100,"sint32":-10,"string":"Hello","uint32":10,"uint64":100}
]`)
	got, err := record.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, string(want), string(got))
}
