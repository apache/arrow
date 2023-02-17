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

package pqarrow_test

import (
	"encoding/base64"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/flight"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/metadata"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"github.com/apache/arrow/go/v12/parquet/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetOriginSchemaBase64(t *testing.T) {
	md := arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"-1"})
	origArrSc := arrow.NewSchema([]arrow.Field{
		{Name: "f1", Type: arrow.BinaryTypes.String, Metadata: md},
		{Name: "f2", Type: arrow.PrimitiveTypes.Int64, Metadata: md},
	}, nil)

	arrSerializedSc := flight.SerializeSchema(origArrSc, memory.DefaultAllocator)
	pqschema, err := pqarrow.ToParquet(origArrSc, nil, pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	tests := []struct {
		name string
		enc  *base64.Encoding
	}{
		{"raw", base64.RawStdEncoding},
		{"std", base64.StdEncoding},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kv := metadata.NewKeyValueMetadata()
			kv.Append("ARROW:schema", tt.enc.EncodeToString(arrSerializedSc))
			arrsc, err := pqarrow.FromParquet(pqschema, nil, kv)
			assert.NoError(t, err)
			assert.True(t, origArrSc.Equal(arrsc))
		})
	}
}

func TestToParquetWriterConfig(t *testing.T) {
	origSc := arrow.NewSchema([]arrow.Field{
		{Name: "f1", Type: arrow.BinaryTypes.String},
		{Name: "f2", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	tests := []struct {
		name           string
		rootRepetition parquet.Repetition
	}{
		{"test1", parquet.Repetitions.Required},
		{"test2", parquet.Repetitions.Repeated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			pqschema, err := pqarrow.ToParquet(origSc,
				parquet.NewWriterProperties(
					parquet.WithRootName(tt.name),
					parquet.WithRootRepetition(tt.rootRepetition),
				),
				pqarrow.DefaultWriterProps())
			require.NoError(t, err)

			assert.Equal(t, tt.name, pqschema.Root().Name())
			assert.Equal(t, tt.rootRepetition, pqschema.Root().RepetitionType())
		})
	}
}

func TestConvertArrowFlatPrimitives(t *testing.T) {
	parquetFields := make(schema.FieldList, 0)
	arrowFields := make([]arrow.Field, 0)

	parquetFields = append(parquetFields, schema.NewBooleanNode("boolean", parquet.Repetitions.Required, -1))
	arrowFields = append(arrowFields, arrow.Field{Name: "boolean", Type: arrow.FixedWidthTypes.Boolean, Nullable: false})

	parquetFields = append(parquetFields, schema.Must(schema.NewPrimitiveNodeLogical("int8", parquet.Repetitions.Required,
		schema.NewIntLogicalType(8, true), parquet.Types.Int32, 0, -1)))
	arrowFields = append(arrowFields, arrow.Field{Name: "int8", Type: arrow.PrimitiveTypes.Int8, Nullable: false})

	parquetFields = append(parquetFields, schema.Must(schema.NewPrimitiveNodeLogical("uint8", parquet.Repetitions.Required,
		schema.NewIntLogicalType(8, false), parquet.Types.Int32, 0, -1)))
	arrowFields = append(arrowFields, arrow.Field{Name: "uint8", Type: arrow.PrimitiveTypes.Uint8, Nullable: false})

	parquetFields = append(parquetFields, schema.Must(schema.NewPrimitiveNodeLogical("int16", parquet.Repetitions.Required,
		schema.NewIntLogicalType(16, true), parquet.Types.Int32, 0, -1)))
	arrowFields = append(arrowFields, arrow.Field{Name: "int16", Type: arrow.PrimitiveTypes.Int16, Nullable: false})

	parquetFields = append(parquetFields, schema.Must(schema.NewPrimitiveNodeLogical("uint16", parquet.Repetitions.Required,
		schema.NewIntLogicalType(16, false), parquet.Types.Int32, 0, -1)))
	arrowFields = append(arrowFields, arrow.Field{Name: "uint16", Type: arrow.PrimitiveTypes.Uint16, Nullable: false})

	parquetFields = append(parquetFields, schema.Must(schema.NewPrimitiveNodeLogical("int32", parquet.Repetitions.Required,
		schema.NewIntLogicalType(32, true), parquet.Types.Int32, 0, -1)))
	arrowFields = append(arrowFields, arrow.Field{Name: "int32", Type: arrow.PrimitiveTypes.Int32, Nullable: false})

	parquetFields = append(parquetFields, schema.Must(schema.NewPrimitiveNodeLogical("uint32", parquet.Repetitions.Required,
		schema.NewIntLogicalType(32, false), parquet.Types.Int32, 0, -1)))
	arrowFields = append(arrowFields, arrow.Field{Name: "uint32", Type: arrow.PrimitiveTypes.Uint32, Nullable: false})

	parquetFields = append(parquetFields, schema.Must(schema.NewPrimitiveNodeLogical("int64", parquet.Repetitions.Required,
		schema.NewIntLogicalType(64, true), parquet.Types.Int64, 0, -1)))
	arrowFields = append(arrowFields, arrow.Field{Name: "int64", Type: arrow.PrimitiveTypes.Int64, Nullable: false})

	parquetFields = append(parquetFields, schema.Must(schema.NewPrimitiveNodeLogical("uint64", parquet.Repetitions.Required,
		schema.NewIntLogicalType(64, false), parquet.Types.Int64, 0, -1)))
	arrowFields = append(arrowFields, arrow.Field{Name: "uint64", Type: arrow.PrimitiveTypes.Uint64, Nullable: false})

	parquetFields = append(parquetFields, schema.Must(schema.NewPrimitiveNodeConverted("timestamp", parquet.Repetitions.Required,
		parquet.Types.Int64, schema.ConvertedTypes.TimestampMillis, 0, 0, 0, -1)))
	arrowFields = append(arrowFields, arrow.Field{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_ms, Nullable: false})

	parquetFields = append(parquetFields, schema.Must(schema.NewPrimitiveNodeConverted("timestamp[us]", parquet.Repetitions.Required,
		parquet.Types.Int64, schema.ConvertedTypes.TimestampMicros, 0, 0, 0, -1)))
	arrowFields = append(arrowFields, arrow.Field{Name: "timestamp[us]", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false})

	parquetFields = append(parquetFields, schema.Must(schema.NewPrimitiveNodeLogical("date", parquet.Repetitions.Required,
		schema.DateLogicalType{}, parquet.Types.Int32, 0, -1)))
	arrowFields = append(arrowFields, arrow.Field{Name: "date", Type: arrow.FixedWidthTypes.Date32, Nullable: false})

	parquetFields = append(parquetFields, schema.Must(schema.NewPrimitiveNodeLogical("date64", parquet.Repetitions.Required,
		schema.NewTimestampLogicalType(true, schema.TimeUnitMillis), parquet.Types.Int64, 0, -1)))
	arrowFields = append(arrowFields, arrow.Field{Name: "date64", Type: arrow.FixedWidthTypes.Date64, Nullable: false})

	parquetFields = append(parquetFields, schema.Must(schema.NewPrimitiveNodeLogical("time32", parquet.Repetitions.Required,
		schema.NewTimeLogicalType(true, schema.TimeUnitMillis), parquet.Types.Int32, 0, -1)))
	arrowFields = append(arrowFields, arrow.Field{Name: "time32", Type: arrow.FixedWidthTypes.Time32ms, Nullable: false})

	parquetFields = append(parquetFields, schema.Must(schema.NewPrimitiveNodeLogical("time64", parquet.Repetitions.Required,
		schema.NewTimeLogicalType(true, schema.TimeUnitMicros), parquet.Types.Int64, 0, -1)))
	arrowFields = append(arrowFields, arrow.Field{Name: "time64", Type: arrow.FixedWidthTypes.Time64us, Nullable: false})

	parquetFields = append(parquetFields, schema.NewInt96Node("timestamp96", parquet.Repetitions.Required, -1))
	arrowFields = append(arrowFields, arrow.Field{Name: "timestamp96", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: false})

	parquetFields = append(parquetFields, schema.NewFloat32Node("float", parquet.Repetitions.Optional, -1))
	arrowFields = append(arrowFields, arrow.Field{Name: "float", Type: arrow.PrimitiveTypes.Float32, Nullable: true})

	parquetFields = append(parquetFields, schema.NewFloat64Node("double", parquet.Repetitions.Optional, -1))
	arrowFields = append(arrowFields, arrow.Field{Name: "double", Type: arrow.PrimitiveTypes.Float64, Nullable: true})

	parquetFields = append(parquetFields, schema.NewByteArrayNode("binary", parquet.Repetitions.Optional, -1))
	arrowFields = append(arrowFields, arrow.Field{Name: "binary", Type: arrow.BinaryTypes.Binary, Nullable: true})

	parquetFields = append(parquetFields, schema.Must(schema.NewPrimitiveNodeLogical("string", parquet.Repetitions.Optional,
		schema.StringLogicalType{}, parquet.Types.ByteArray, 0, -1)))
	arrowFields = append(arrowFields, arrow.Field{Name: "string", Type: arrow.BinaryTypes.String, Nullable: true})

	parquetFields = append(parquetFields, schema.NewFixedLenByteArrayNode("flba-binary", parquet.Repetitions.Optional, 12, -1))
	arrowFields = append(arrowFields, arrow.Field{Name: "flba-binary", Type: &arrow.FixedSizeBinaryType{ByteWidth: 12}, Nullable: true})

	arrowSchema := arrow.NewSchema(arrowFields, nil)
	parquetSchema := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, parquetFields, -1)))

	result, err := pqarrow.ToParquet(arrowSchema, nil, pqarrow.NewArrowWriterProperties(pqarrow.WithDeprecatedInt96Timestamps(true)))
	assert.NoError(t, err)
	assert.True(t, parquetSchema.Equals(result))
	for i := 0; i < parquetSchema.NumColumns(); i++ {
		assert.Truef(t, parquetSchema.Column(i).Equals(result.Column(i)), "Column %d didn't match: %s", i, parquetSchema.Column(i).Name())
	}
}

func TestConvertArrowParquetLists(t *testing.T) {
	parquetFields := make(schema.FieldList, 0)
	arrowFields := make([]arrow.Field, 0)

	parquetFields = append(parquetFields, schema.MustGroup(schema.ListOf(schema.Must(schema.NewPrimitiveNodeLogical("my_list",
		parquet.Repetitions.Optional, schema.StringLogicalType{}, parquet.Types.ByteArray, 0, -1)), parquet.Repetitions.Required, -1)))

	arrowFields = append(arrowFields, arrow.Field{Name: "my_list", Type: arrow.ListOf(arrow.BinaryTypes.String)})

	parquetFields = append(parquetFields, schema.MustGroup(schema.ListOf(schema.Must(schema.NewPrimitiveNodeLogical("my_list",
		parquet.Repetitions.Optional, schema.StringLogicalType{}, parquet.Types.ByteArray, 0, -1)), parquet.Repetitions.Optional, -1)))

	arrowFields = append(arrowFields, arrow.Field{Name: "my_list", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true})

	arrowSchema := arrow.NewSchema(arrowFields, nil)
	parquetSchema := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, parquetFields, -1)))

	result, err := pqarrow.ToParquet(arrowSchema, nil, pqarrow.NewArrowWriterProperties(pqarrow.WithDeprecatedInt96Timestamps(true)))
	assert.NoError(t, err)
	assert.True(t, parquetSchema.Equals(result), parquetSchema.String(), result.String())
	for i := 0; i < parquetSchema.NumColumns(); i++ {
		assert.Truef(t, parquetSchema.Column(i).Equals(result.Column(i)), "Column %d didn't match: %s", i, parquetSchema.Column(i).Name())
	}
}

func TestConvertArrowDecimals(t *testing.T) {
	parquetFields := make(schema.FieldList, 0)
	arrowFields := make([]arrow.Field, 0)

	parquetFields = append(parquetFields, schema.Must(schema.NewPrimitiveNodeLogical("decimal_8_4", parquet.Repetitions.Required,
		schema.NewDecimalLogicalType(8, 4), parquet.Types.FixedLenByteArray, 4, -1)))
	arrowFields = append(arrowFields, arrow.Field{Name: "decimal_8_4", Type: &arrow.Decimal128Type{Precision: 8, Scale: 4}})

	parquetFields = append(parquetFields, schema.Must(schema.NewPrimitiveNodeLogical("decimal_20_4", parquet.Repetitions.Required,
		schema.NewDecimalLogicalType(20, 4), parquet.Types.FixedLenByteArray, 9, -1)))
	arrowFields = append(arrowFields, arrow.Field{Name: "decimal_20_4", Type: &arrow.Decimal128Type{Precision: 20, Scale: 4}})

	parquetFields = append(parquetFields, schema.Must(schema.NewPrimitiveNodeLogical("decimal_77_4", parquet.Repetitions.Required,
		schema.NewDecimalLogicalType(77, 4), parquet.Types.FixedLenByteArray, 34, -1)))
	arrowFields = append(arrowFields, arrow.Field{Name: "decimal_77_4", Type: &arrow.Decimal128Type{Precision: 77, Scale: 4}})

	arrowSchema := arrow.NewSchema(arrowFields, nil)
	parquetSchema := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, parquetFields, -1)))

	result, err := pqarrow.ToParquet(arrowSchema, nil, pqarrow.NewArrowWriterProperties(pqarrow.WithDeprecatedInt96Timestamps(true)))
	assert.NoError(t, err)
	assert.True(t, parquetSchema.Equals(result))
	for i := 0; i < parquetSchema.NumColumns(); i++ {
		assert.Truef(t, parquetSchema.Column(i).Equals(result.Column(i)), "Column %d didn't match: %s", i, parquetSchema.Column(i).Name())
	}
}

func TestCoerceTImestampV1(t *testing.T) {
	parquetFields := make(schema.FieldList, 0)
	arrowFields := make([]arrow.Field, 0)

	parquetFields = append(parquetFields, schema.Must(schema.NewPrimitiveNodeLogical("timestamp", parquet.Repetitions.Required,
		schema.NewTimestampLogicalTypeForce(false, schema.TimeUnitMicros), parquet.Types.Int64, 0, -1)))
	arrowFields = append(arrowFields, arrow.Field{Name: "timestamp", Type: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "EST"}})

	arrowSchema := arrow.NewSchema(arrowFields, nil)
	parquetSchema := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, parquetFields, -1)))

	result, err := pqarrow.ToParquet(arrowSchema, parquet.NewWriterProperties(parquet.WithVersion(parquet.V1_0)), pqarrow.NewArrowWriterProperties(pqarrow.WithCoerceTimestamps(arrow.Microsecond)))
	assert.NoError(t, err)
	assert.True(t, parquetSchema.Equals(result))
	for i := 0; i < parquetSchema.NumColumns(); i++ {
		assert.Truef(t, parquetSchema.Column(i).Equals(result.Column(i)), "Column %d didn't match: %s", i, parquetSchema.Column(i).Name())
	}
}

func TestAutoCoerceTImestampV1(t *testing.T) {
	parquetFields := make(schema.FieldList, 0)
	arrowFields := make([]arrow.Field, 0)

	parquetFields = append(parquetFields, schema.Must(schema.NewPrimitiveNodeLogical("timestamp", parquet.Repetitions.Required,
		schema.NewTimestampLogicalTypeForce(false, schema.TimeUnitMicros), parquet.Types.Int64, 0, -1)))
	arrowFields = append(arrowFields, arrow.Field{Name: "timestamp", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "EST"}})

	parquetFields = append(parquetFields, schema.Must(schema.NewPrimitiveNodeLogical("timestamp[ms]", parquet.Repetitions.Required,
		schema.NewTimestampLogicalTypeForce(true, schema.TimeUnitMillis), parquet.Types.Int64, 0, -1)))
	arrowFields = append(arrowFields, arrow.Field{Name: "timestamp[ms]", Type: &arrow.TimestampType{Unit: arrow.Second}})

	arrowSchema := arrow.NewSchema(arrowFields, nil)
	parquetSchema := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, parquetFields, -1)))

	result, err := pqarrow.ToParquet(arrowSchema, parquet.NewWriterProperties(parquet.WithVersion(parquet.V1_0)), pqarrow.NewArrowWriterProperties())
	assert.NoError(t, err)
	assert.True(t, parquetSchema.Equals(result))
	for i := 0; i < parquetSchema.NumColumns(); i++ {
		assert.Truef(t, parquetSchema.Column(i).Equals(result.Column(i)), "Column %d didn't match: %s", i, parquetSchema.Column(i).Name())
	}
}

func TestConvertArrowStruct(t *testing.T) {
	parquetFields := make(schema.FieldList, 0)
	arrowFields := make([]arrow.Field, 0)

	parquetFields = append(parquetFields, schema.Must(schema.NewPrimitiveNodeLogical("leaf1", parquet.Repetitions.Optional, schema.NewIntLogicalType(32, true), parquet.Types.Int32, 0, -1)))
	parquetFields = append(parquetFields, schema.Must(schema.NewGroupNode("outerGroup", parquet.Repetitions.Required, schema.FieldList{
		schema.Must(schema.NewPrimitiveNodeLogical("leaf2", parquet.Repetitions.Optional, schema.NewIntLogicalType(32, true), parquet.Types.Int32, 0, -1)),
		schema.Must(schema.NewGroupNode("innerGroup", parquet.Repetitions.Required, schema.FieldList{
			schema.Must(schema.NewPrimitiveNodeLogical("leaf3", parquet.Repetitions.Optional, schema.NewIntLogicalType(32, true), parquet.Types.Int32, 0, -1)),
		}, -1)),
	}, -1)))

	arrowFields = append(arrowFields, arrow.Field{Name: "leaf1", Type: arrow.PrimitiveTypes.Int32, Nullable: true})
	arrowFields = append(arrowFields, arrow.Field{Name: "outerGroup", Type: arrow.StructOf(
		arrow.Field{Name: "leaf2", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "innerGroup", Type: arrow.StructOf(
			arrow.Field{Name: "leaf3", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		)},
	)})

	arrowSchema := arrow.NewSchema(arrowFields, nil)
	parquetSchema := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, parquetFields, -1)))

	result, err := pqarrow.ToParquet(arrowSchema, nil, pqarrow.NewArrowWriterProperties())
	assert.NoError(t, err)
	assert.True(t, parquetSchema.Equals(result))
	for i := 0; i < parquetSchema.NumColumns(); i++ {
		assert.Truef(t, parquetSchema.Column(i).Equals(result.Column(i)), "Column %d didn't match: %s", i, parquetSchema.Column(i).Name())
	}
}

func TestListStructBackwardCompatible(t *testing.T) {
	// Set up old construction for list of struct, not using
	// the 3-level encoding. Schema looks like:
	//
	//     required group field_id=-1 root {
	//       optional group field_id=-1 answers (List) {
	//		   repeated group field_id=-1 array {
	//           optional byte_array field_id=-1 type (String);
	//           optional byte_array field_id=-1 rdata (String);
	//           optional byte_array field_id=-1 class (String);
	//         }
	//       }
	//     }
	//
	// Instaed of the proper 3-level encoding which would be:
	//
	//     repeated group field_id=-1 schema {
	//       optional group field_id=-1 answers (List) {
	//         repeated group field_id=-1 list {
	//           optional group field_id=-1 element {
	//             optional byte_array field_id=-1 type (String);
	//             optional byte_array field_id=-1 rdata (String);
	//             optional byte_array field_id=-1 class (String);
	//           }
	//         }
	//       }
	//     }
	//
	pqSchema := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("root", parquet.Repetitions.Required, schema.FieldList{
		schema.Must(schema.NewGroupNodeLogical("answers", parquet.Repetitions.Optional, schema.FieldList{
			schema.Must(schema.NewGroupNode("array", parquet.Repetitions.Repeated, schema.FieldList{
				schema.MustPrimitive(schema.NewPrimitiveNodeLogical("type", parquet.Repetitions.Optional,
					schema.StringLogicalType{}, parquet.Types.ByteArray, -1, -1)),
				schema.MustPrimitive(schema.NewPrimitiveNodeLogical("rdata", parquet.Repetitions.Optional,
					schema.StringLogicalType{}, parquet.Types.ByteArray, -1, -1)),
				schema.MustPrimitive(schema.NewPrimitiveNodeLogical("class", parquet.Repetitions.Optional,
					schema.StringLogicalType{}, parquet.Types.ByteArray, -1, -1)),
			}, -1)),
		}, schema.NewListLogicalType(), -1)),
	}, -1)))

	meta := arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"-1"})
	// desired equivalent arrow schema would be list<item: struct<type: utf8, rdata: utf8, class: utf8>>
	arrowSchema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "answers", Type: arrow.ListOfField(arrow.Field{
				Name: "array", Type: arrow.StructOf(
					arrow.Field{Name: "type", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: meta},
					arrow.Field{Name: "rdata", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: meta},
					arrow.Field{Name: "class", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: meta},
				), Nullable: true}), Nullable: true, Metadata: meta},
		}, nil)

	arrsc, err := pqarrow.FromParquet(pqSchema, nil, metadata.KeyValueMetadata{})
	assert.NoError(t, err)
	assert.True(t, arrowSchema.Equal(arrsc))
}
