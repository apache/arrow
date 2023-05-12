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

package schema_test

import (
	"log"
	"os"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/schema"
	"github.com/stretchr/testify/assert"
)

func ExampleNewSchemaFromStruct_primitives() {
	type Schema struct {
		Bool              bool
		Int8              int8
		Uint16            uint16
		Int32             int32
		Int64             int64
		Int96             parquet.Int96
		Float             float32
		Double            float64
		ByteArray         string
		FixedLenByteArray [10]byte
	}

	sc, err := schema.NewSchemaFromStruct(Schema{})
	if err != nil {
		log.Fatal(err)
	}

	schema.PrintSchema(sc.Root(), os.Stdout, 2)

	// Output:
	// repeated group field_id=-1 Schema {
	//   required boolean field_id=-1 Bool;
	//   required int32 field_id=-1 Int8 (Int(bitWidth=8, isSigned=true));
	//   required int32 field_id=-1 Uint16 (Int(bitWidth=16, isSigned=false));
	//   required int32 field_id=-1 Int32 (Int(bitWidth=32, isSigned=true));
	//   required int64 field_id=-1 Int64 (Int(bitWidth=64, isSigned=true));
	//   required int96 field_id=-1 Int96;
	//   required float field_id=-1 Float;
	//   required double field_id=-1 Double;
	//   required byte_array field_id=-1 ByteArray;
	//   required fixed_len_byte_array field_id=-1 FixedLenByteArray;
	// }
}

func ExampleNewSchemaFromStruct_convertedtypes() {
	type ConvertedSchema struct {
		Utf8           string        `parquet:"name=utf8, converted=UTF8"`
		Uint32         uint32        `parquet:"converted=INT_32"`
		Date           int32         `parquet:"name=date, converted=date"`
		TimeMilli      int32         `parquet:"name=timemilli, converted=TIME_MILLIS"`
		TimeMicro      int64         `parquet:"name=timemicro, converted=time_micros"`
		TimeStampMilli int64         `parquet:"converted=timestamp_millis"`
		TimeStampMicro int64         `parquet:"converted=timestamp_micros"`
		Interval       parquet.Int96 `parquet:"converted=INTERVAL"`
		Decimal1       int32         `parquet:"converted=decimal, scale=2, precision=9"`
		Decimal2       int64         `parquet:"converted=decimal, scale=2, precision=18"`
		Decimal3       [12]byte      `parquet:"converted=decimal, scale=2, precision=10"`
		Decimal4       string        `parquet:"converted=decimal, scale=2, precision=20"`
	}

	sc, err := schema.NewSchemaFromStruct(&ConvertedSchema{})
	if err != nil {
		log.Fatal(err)
	}

	schema.PrintSchema(sc.Root(), os.Stdout, 2)

	// Output:
	// repeated group field_id=-1 ConvertedSchema {
	//   required byte_array field_id=-1 utf8 (String);
	//   required int32 field_id=-1 Uint32 (Int(bitWidth=32, isSigned=true));
	//   required int32 field_id=-1 date (Date);
	//   required int32 field_id=-1 timemilli (Time(isAdjustedToUTC=true, timeUnit=milliseconds));
	//   required int64 field_id=-1 timemicro (Time(isAdjustedToUTC=true, timeUnit=microseconds));
	//   required int64 field_id=-1 TimeStampMilli (Timestamp(isAdjustedToUTC=true, timeUnit=milliseconds, is_from_converted_type=true, force_set_converted_type=false));
	//   required int64 field_id=-1 TimeStampMicro (Timestamp(isAdjustedToUTC=true, timeUnit=microseconds, is_from_converted_type=true, force_set_converted_type=false));
	//   required int96 field_id=-1 Interval;
	//   required int32 field_id=-1 Decimal1 (Decimal(precision=9, scale=2));
	//   required int64 field_id=-1 Decimal2 (Decimal(precision=18, scale=2));
	//   required fixed_len_byte_array field_id=-1 Decimal3 (Decimal(precision=10, scale=2));
	//   required byte_array field_id=-1 Decimal4 (Decimal(precision=20, scale=2));
	// }
}

func ExampleNewSchemaFromStruct_repetition() {
	type RepetitionSchema struct {
		List     []int64 `parquet:"fieldid=1"`
		Repeated []int64 `parquet:"repetition=repeated, fieldid=2"`
		Optional *int64  `parquet:"fieldid=3"`
		Required *int64  `parquet:"repetition=REQUIRED, fieldid=4"`
		Opt      int64   `parquet:"repetition=OPTIONAL, fieldid=5"`
	}

	sc, err := schema.NewSchemaFromStruct(RepetitionSchema{})
	if err != nil {
		log.Fatal(err)
	}

	schema.PrintSchema(sc.Root(), os.Stdout, 2)

	// Output:
	// repeated group field_id=-1 RepetitionSchema {
	//   required group field_id=1 List (List) {
	//     repeated group field_id=-1 list {
	//       required int64 field_id=-1 element (Int(bitWidth=64, isSigned=true));
	//     }
	//   }
	//   repeated int64 field_id=2 Repeated (Int(bitWidth=64, isSigned=true));
	//   optional int64 field_id=3 Optional (Int(bitWidth=64, isSigned=true));
	//   required int64 field_id=4 Required (Int(bitWidth=64, isSigned=true));
	//   optional int64 field_id=5 Opt (Int(bitWidth=64, isSigned=true));
	// }
}

func ExampleNewSchemaFromStruct_logicaltypes() {
	type LogicalTypes struct {
		String                []byte   `parquet:"logical=String"`
		Enum                  string   `parquet:"logical=enum"`
		Date                  int32    `parquet:"logical=date"`
		Decimal1              int32    `parquet:"logical=decimal, precision=9, scale=2"`
		Decimal2              int32    `parquet:"logical=decimal, logical.precision=9, scale=2"`
		Decimal3              int32    `parquet:"logical=decimal, precision=5, logical.precision=9, scale=1, logical.scale=3"`
		TimeMilliUTC          int32    `parquet:"logical=TIME, logical.unit=millis"`
		TimeMilli             int32    `parquet:"logical=Time, logical.unit=millis, logical.isadjustedutc=false"`
		TimeMicros            int64    `parquet:"logical=time, logical.unit=micros, logical.isadjustedutc=false"`
		TimeMicrosUTC         int64    `parquet:"logical=time, logical.unit=micros, logical.isadjustedutc=true"`
		TimeNanos             int64    `parquet:"logical=time, logical.unit=nanos"`
		TimestampMilli        int64    `parquet:"logical=timestamp, logical.unit=millis"`
		TimestampMicrosNotUTC int64    `parquet:"logical=timestamp, logical.unit=micros, logical.isadjustedutc=false"`
		TimestampNanos        int64    `parquet:"logical=timestamp, logical.unit=nanos"`
		JSON                  string   `parquet:"logical=json"`
		BSON                  []byte   `parquet:"logical=BSON"`
		UUID                  [16]byte `parquet:"logical=uuid"`
	}

	sc, err := schema.NewSchemaFromStruct(LogicalTypes{})
	if err != nil {
		log.Fatal(err)
	}

	schema.PrintSchema(sc.Root(), os.Stdout, 2)

	// Output:
	// repeated group field_id=-1 LogicalTypes {
	//   required byte_array field_id=-1 String (String);
	//   required byte_array field_id=-1 Enum (Enum);
	//   required int32 field_id=-1 Date (Date);
	//   required int32 field_id=-1 Decimal1 (Decimal(precision=9, scale=2));
	//   required int32 field_id=-1 Decimal2 (Decimal(precision=9, scale=2));
	//   required int32 field_id=-1 Decimal3 (Decimal(precision=9, scale=3));
	//   required int32 field_id=-1 TimeMilliUTC (Time(isAdjustedToUTC=true, timeUnit=milliseconds));
	//   required int32 field_id=-1 TimeMilli (Time(isAdjustedToUTC=false, timeUnit=milliseconds));
	//   required int64 field_id=-1 TimeMicros (Time(isAdjustedToUTC=false, timeUnit=microseconds));
	//   required int64 field_id=-1 TimeMicrosUTC (Time(isAdjustedToUTC=true, timeUnit=microseconds));
	//   required int64 field_id=-1 TimeNanos (Time(isAdjustedToUTC=true, timeUnit=nanoseconds));
	//   required int64 field_id=-1 TimestampMilli (Timestamp(isAdjustedToUTC=true, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false));
	//   required int64 field_id=-1 TimestampMicrosNotUTC (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false));
	//   required int64 field_id=-1 TimestampNanos (Timestamp(isAdjustedToUTC=true, timeUnit=nanoseconds, is_from_converted_type=false, force_set_converted_type=false));
	//   required byte_array field_id=-1 JSON (JSON);
	//   required byte_array field_id=-1 BSON (BSON);
	//   required fixed_len_byte_array field_id=-1 UUID (UUID);
	// }
}

func ExampleNewSchemaFromStruct_physicaltype() {
	type ChangeTypes struct {
		Int32        int64  `parquet:"type=int32"`
		FixedLen     string `parquet:"type=fixed_len_byte_array, length=10"`
		SliceAsFixed []byte `parquet:"type=fixed_len_byte_array, length=12"`
		Int          int    `parquet:"type=int32"`
	}

	sc, err := schema.NewSchemaFromStruct(ChangeTypes{})
	if err != nil {
		log.Fatal(err)
	}

	schema.PrintSchema(sc.Root(), os.Stdout, 2)

	// Output:
	// repeated group field_id=-1 ChangeTypes {
	//   required int32 field_id=-1 Int32 (Int(bitWidth=32, isSigned=true));
	//   required fixed_len_byte_array field_id=-1 FixedLen;
	//   required fixed_len_byte_array field_id=-1 SliceAsFixed;
	//   required int32 field_id=-1 Int (Int(bitWidth=32, isSigned=true));
	// }
}

func ExampleNewSchemaFromStruct_nestedtypes() {
	type Other struct {
		OptionalMap *map[string]*string `parquet:"valuerepetition=required, keylogical=String, valueconverted=BSON"`
	}

	type MyMap map[int32]string

	type Nested struct {
		SimpleMap     map[int32]string
		FixedLenMap   map[string][]byte `parquet:"keytype=fixed_len_byte_array, keyfieldid=10, valuefieldid=11, keylength=10"`
		DecimalMap    map[int32]string  `parquet:"logical=map, keyconverted=DECIMAL, keyscale=3, keyprecision=7, valuetype=fixed_len_byte_array, valuelength=4, valuelogical=decimal, valuelogical.precision=9, valuescale=2"`
		OtherList     []*Other
		OtherRepeated []Other  `parquet:"repetition=repeated"`
		DateArray     [5]int32 `parquet:"valuelogical=date, logical=list"`
		DateMap       MyMap    `parquet:"keylogical=TIME, keylogical.unit=MILLIS, keylogical.isadjustedutc=false, valuelogical=enum"`
	}

	sc, err := schema.NewSchemaFromStruct(Nested{})
	if err != nil {
		log.Fatal(err)
	}

	schema.PrintSchema(sc.Root(), os.Stdout, 2)

	// Output:
	// repeated group field_id=-1 Nested {
	//   required group field_id=-1 SimpleMap (Map) {
	//     repeated group field_id=-1 key_value {
	//       required int32 field_id=-1 key (Int(bitWidth=32, isSigned=true));
	//       required byte_array field_id=-1 value;
	//     }
	//   }
	//   required group field_id=-1 FixedLenMap (Map) {
	//     repeated group field_id=-1 key_value {
	//       required fixed_len_byte_array field_id=10 key;
	//       required byte_array field_id=11 value;
	//     }
	//   }
	//   required group field_id=-1 DecimalMap (Map) {
	//     repeated group field_id=-1 key_value {
	//       required int32 field_id=-1 key (Decimal(precision=7, scale=3));
	//       required fixed_len_byte_array field_id=-1 value (Decimal(precision=9, scale=2));
	//     }
	//   }
	//   required group field_id=-1 OtherList (List) {
	//     repeated group field_id=-1 list {
	//       optional group field_id=-1 element {
	//         optional group field_id=-1 OptionalMap (Map) {
	//           repeated group field_id=-1 key_value {
	//             required byte_array field_id=-1 key (String);
	//             required byte_array field_id=-1 value (BSON);
	//           }
	//         }
	//       }
	//     }
	//   }
	//   repeated group field_id=-1 OtherRepeated {
	//     optional group field_id=-1 OptionalMap (Map) {
	//       repeated group field_id=-1 key_value {
	//         required byte_array field_id=-1 key (String);
	//         required byte_array field_id=-1 value (BSON);
	//       }
	//     }
	//   }
	//   required group field_id=-1 DateArray (List) {
	//     repeated group field_id=-1 list {
	//       required int32 field_id=-1 element (Date);
	//     }
	//   }
	//   required group field_id=-1 DateMap (Map) {
	//     repeated group field_id=-1 key_value {
	//       required int32 field_id=-1 key (Time(isAdjustedToUTC=false, timeUnit=milliseconds));
	//       required byte_array field_id=-1 value (Enum);
	//     }
	//   }
	// }
}

func TestStructFromSchema(t *testing.T) {
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{
		schema.NewBooleanNode("bool", parquet.Repetitions.Required, -1),
		schema.NewInt32Node("int32", parquet.Repetitions.Optional, -1),
		schema.NewInt64Node("int64", parquet.Repetitions.Repeated, -1),
		schema.NewInt96Node("int96", parquet.Repetitions.Required, -1),
		schema.NewFloat32Node("float", parquet.Repetitions.Required, -1),
		schema.NewByteArrayNode("bytearray", parquet.Repetitions.Required, -1),
		schema.NewFixedLenByteArrayNode("fixedLen", parquet.Repetitions.Required, 10, -1),
	}, -1)
	assert.NoError(t, err)

	sc := schema.NewSchema(root)

	typ, err := schema.NewStructFromSchema(sc)
	assert.NoError(t, err)

	assert.Equal(t, reflect.Struct, typ.Kind())
	assert.Equal(t, "struct { bool bool; int32 *int32; int64 []int64; int96 parquet.Int96; float float32; bytearray parquet.ByteArray; fixedLen parquet.FixedLenByteArray }",
		typ.String())
}

func TestStructFromSchemaWithNesting(t *testing.T) {
	type Other struct {
		List *[]*float32
	}

	type Nested struct {
		Nest         []int32
		OptionalNest []*int64
		Mapped       map[string]float32
		Other        []Other
		Other2       Other
	}

	sc, err := schema.NewSchemaFromStruct(Nested{})
	assert.NoError(t, err)

	typ, err := schema.NewStructFromSchema(sc)
	assert.NoError(t, err)
	assert.Equal(t, "struct { Nest []int32; OptionalNest []*int64; Mapped map[string]float32; Other []struct { List *[]*float32 }; Other2 struct { List *[]*float32 } }",
		typ.String())
}

func TestStructFromSchemaBackwardsCompatList(t *testing.T) {
	tests := []struct {
		name     string
		n        schema.Node
		expected string
	}{
		{"proper list", schema.MustGroup(schema.NewGroupNodeLogical("my_list", parquet.Repetitions.Required,
			schema.FieldList{
				schema.MustGroup(schema.NewGroupNode("list", parquet.Repetitions.Repeated, schema.FieldList{schema.NewBooleanNode("element", parquet.Repetitions.Optional, -1)}, -1)),
			}, schema.NewListLogicalType(), -1)), "struct { my_list []*bool }"},
		{"backward nullable list nonnull ints", schema.MustGroup(schema.NewGroupNodeLogical("my_list", parquet.Repetitions.Optional, schema.FieldList{
			schema.NewInt32Node("element", parquet.Repetitions.Repeated, -1),
		}, schema.NewListLogicalType(), -1)), "struct { my_list *[]int32 }"},
		{"backward nullable list tuple string int", schema.MustGroup(schema.NewGroupNodeLogical("my_list", parquet.Repetitions.Optional, schema.FieldList{
			schema.MustGroup(schema.NewGroupNode("element", parquet.Repetitions.Repeated, schema.FieldList{
				schema.MustPrimitive(schema.NewPrimitiveNodeLogical("str", parquet.Repetitions.Required, schema.StringLogicalType{}, parquet.Types.ByteArray, 0, -1)),
				schema.NewInt32Node("num", parquet.Repetitions.Required, -1),
			}, -1)),
		}, schema.NewListLogicalType(), -1)), "struct { my_list *[]struct { str string; num int32 } }"},
		{"list tuple string", schema.MustGroup(schema.NewGroupNodeLogical("my_list", parquet.Repetitions.Required, schema.FieldList{
			schema.MustGroup(schema.NewGroupNode("array", parquet.Repetitions.Repeated, schema.FieldList{
				schema.NewByteArrayNode("str", parquet.Repetitions.Required, -1),
			}, -1)),
		}, schema.NewListLogicalType(), -1)), "struct { my_list []struct { str parquet.ByteArray } }"},
		{"list tuple string my_list_tuple", schema.MustGroup(schema.NewGroupNodeLogical("my_list", parquet.Repetitions.Optional, schema.FieldList{
			schema.MustGroup(schema.NewGroupNode("my_list_tuple", parquet.Repetitions.Repeated, schema.FieldList{
				schema.MustPrimitive(schema.NewPrimitiveNodeLogical("str", parquet.Repetitions.Required, schema.StringLogicalType{}, parquet.Types.ByteArray, 0, -1)),
			}, -1)),
		}, schema.NewListLogicalType(), -1)), "struct { my_list *[]struct { str string } }"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			typ, err := schema.NewStructFromSchema(schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{tt.n}, -1))))
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, typ.String())
		})
	}
}

func TestStructFromSchemaMaps(t *testing.T) {
	tests := []struct {
		name     string
		n        schema.Node
		expected string
	}{
		{"map string int", schema.MustGroup(schema.NewGroupNodeLogical("my_map", parquet.Repetitions.Required, schema.FieldList{
			schema.MustGroup(schema.NewGroupNode("key_value", parquet.Repetitions.Repeated, schema.FieldList{
				schema.MustPrimitive(schema.NewPrimitiveNodeLogical("key", parquet.Repetitions.Required, schema.StringLogicalType{}, parquet.Types.ByteArray, 0, -1)),
				schema.NewInt32Node("value", parquet.Repetitions.Optional, -1),
			}, -1)),
		}, schema.MapLogicalType{}, -1)), "struct { my_map map[string]*int32 }"},
		{"nullable map string, int, required values", schema.MustGroup(schema.NewGroupNodeLogical("my_map", parquet.Repetitions.Optional, schema.FieldList{
			schema.MustGroup(schema.NewGroupNode("map", parquet.Repetitions.Repeated, schema.FieldList{
				schema.NewByteArrayNode("str", parquet.Repetitions.Required, -1),
				schema.NewInt32Node("num", parquet.Repetitions.Required, -1),
			}, -1)),
		}, schema.MapLogicalType{}, -1)), "struct { my_map *map[string]int32 }"},
		{"map_key_value with missing value", schema.MustGroup(schema.NewGroupNodeConverted("my_map", parquet.Repetitions.Optional, schema.FieldList{
			schema.MustGroup(schema.NewGroupNode("map", parquet.Repetitions.Repeated, schema.FieldList{
				schema.NewByteArrayNode("key", parquet.Repetitions.Required, -1),
			}, -1)),
		}, schema.ConvertedTypes.MapKeyValue, -1)), "struct { my_map *map[string]bool }"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			typ, err := schema.NewStructFromSchema(schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{tt.n}, -1))))
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, typ.String())
		})
	}
}
