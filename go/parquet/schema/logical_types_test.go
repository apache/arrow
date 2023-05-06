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
	"encoding/json"
	"testing"

	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/schema"
	"github.com/stretchr/testify/assert"
)

func TestConvertedLogicalEquivalences(t *testing.T) {
	tests := []struct {
		name      string
		converted schema.ConvertedType
		logical   schema.LogicalType
		expected  schema.LogicalType
	}{
		{"utf8", schema.ConvertedTypes.UTF8, schema.StringLogicalType{}, schema.StringLogicalType{}},
		{"map", schema.ConvertedTypes.Map, schema.MapLogicalType{}, schema.MapLogicalType{}},
		{"mapkeyval", schema.ConvertedTypes.MapKeyValue, schema.MapLogicalType{}, schema.MapLogicalType{}},
		{"list", schema.ConvertedTypes.List, schema.NewListLogicalType(), schema.NewListLogicalType()},
		{"enum", schema.ConvertedTypes.Enum, schema.EnumLogicalType{}, schema.EnumLogicalType{}},
		{"date", schema.ConvertedTypes.Date, schema.DateLogicalType{}, schema.DateLogicalType{}},
		{"timemilli", schema.ConvertedTypes.TimeMillis, schema.NewTimeLogicalType(true /* adjustedToUTC */, schema.TimeUnitMillis), &schema.TimeLogicalType{}},
		{"timemicro", schema.ConvertedTypes.TimeMicros, schema.NewTimeLogicalType(true /* adjustedToUTC */, schema.TimeUnitMicros), &schema.TimeLogicalType{}},
		{"timestampmilli", schema.ConvertedTypes.TimestampMillis, schema.NewTimestampLogicalType(true /* adjustedToUTC */, schema.TimeUnitMillis), &schema.TimestampLogicalType{}},
		{"timestampmicro", schema.ConvertedTypes.TimestampMicros, schema.NewTimestampLogicalType(true /* adjustedToUTC */, schema.TimeUnitMicros), &schema.TimestampLogicalType{}},
		{"uint8", schema.ConvertedTypes.Uint8, schema.NewIntLogicalType(8 /* bitWidth */, false /* signed */), &schema.IntLogicalType{}},
		{"uint16", schema.ConvertedTypes.Uint16, schema.NewIntLogicalType(16 /* bitWidth */, false /* signed */), &schema.IntLogicalType{}},
		{"uint32", schema.ConvertedTypes.Uint32, schema.NewIntLogicalType(32 /* bitWidth */, false /* signed */), &schema.IntLogicalType{}},
		{"uint64", schema.ConvertedTypes.Uint64, schema.NewIntLogicalType(64 /* bitWidth */, false /* signed */), &schema.IntLogicalType{}},
		{"int8", schema.ConvertedTypes.Int8, schema.NewIntLogicalType(8 /* bitWidth */, true /* signed */), &schema.IntLogicalType{}},
		{"int16", schema.ConvertedTypes.Int16, schema.NewIntLogicalType(16 /* bitWidth */, true /* signed */), &schema.IntLogicalType{}},
		{"int32", schema.ConvertedTypes.Int32, schema.NewIntLogicalType(32 /* bitWidth */, true /* signed */), &schema.IntLogicalType{}},
		{"int64", schema.ConvertedTypes.Int64, schema.NewIntLogicalType(64 /* bitWidth */, true /* signed */), &schema.IntLogicalType{}},
		{"json", schema.ConvertedTypes.JSON, schema.JSONLogicalType{}, schema.JSONLogicalType{}},
		{"bson", schema.ConvertedTypes.BSON, schema.BSONLogicalType{}, schema.BSONLogicalType{}},
		{"interval", schema.ConvertedTypes.Interval, schema.IntervalLogicalType{}, schema.IntervalLogicalType{}},
		{"none", schema.ConvertedTypes.None, schema.NoLogicalType{}, schema.NoLogicalType{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fromConverted := tt.converted.ToLogicalType(schema.DecimalMetadata{})
			assert.IsType(t, tt.logical, fromConverted)
			assert.True(t, fromConverted.Equals(tt.logical))
			assert.IsType(t, tt.expected, fromConverted)
			assert.IsType(t, tt.expected, tt.logical)
		})
	}

	t.Run("decimal", func(t *testing.T) {
		decimalMeta := schema.DecimalMetadata{IsSet: true, Precision: 10, Scale: 4}
		fromConverted := schema.ConvertedTypes.Decimal.ToLogicalType(decimalMeta)
		fromMake := schema.NewDecimalLogicalType(10, 4)
		assert.IsType(t, fromMake, fromConverted)
		assert.True(t, fromConverted.Equals(fromMake))
		assert.IsType(t, &schema.DecimalLogicalType{}, fromConverted)
		assert.IsType(t, &schema.DecimalLogicalType{}, fromMake)
		assert.True(t, schema.NewDecimalLogicalType(16, 0).Equals(schema.NewDecimalLogicalType(16, 0)))
	})
}

func TestConvertedTypeCompatibility(t *testing.T) {
	tests := []struct {
		name            string
		logical         schema.LogicalType
		expectConverted schema.ConvertedType
	}{
		{"utf8", schema.StringLogicalType{}, schema.ConvertedTypes.UTF8},
		{"map", schema.MapLogicalType{}, schema.ConvertedTypes.Map},
		{"list", schema.NewListLogicalType(), schema.ConvertedTypes.List},
		{"enum", schema.EnumLogicalType{}, schema.ConvertedTypes.Enum},
		{"date", schema.DateLogicalType{}, schema.ConvertedTypes.Date},
		{"time_milli", schema.NewTimeLogicalType(true /* adjutedToUTC */, schema.TimeUnitMillis), schema.ConvertedTypes.TimeMillis},
		{"time_micro", schema.NewTimeLogicalType(true /* adjutedToUTC */, schema.TimeUnitMicros), schema.ConvertedTypes.TimeMicros},
		{"timestamp_milli", schema.NewTimestampLogicalType(true /* adjutedToUTC */, schema.TimeUnitMillis), schema.ConvertedTypes.TimestampMillis},
		{"timestamp_micro", schema.NewTimestampLogicalType(true /* adjutedToUTC */, schema.TimeUnitMicros), schema.ConvertedTypes.TimestampMicros},
		{"uint8", schema.NewIntLogicalType(8 /* bitWidth */, false /* signed */), schema.ConvertedTypes.Uint8},
		{"uint16", schema.NewIntLogicalType(16 /* bitWidth */, false /* signed */), schema.ConvertedTypes.Uint16},
		{"uint32", schema.NewIntLogicalType(32 /* bitWidth */, false /* signed */), schema.ConvertedTypes.Uint32},
		{"uint64", schema.NewIntLogicalType(64 /* bitWidth */, false /* signed */), schema.ConvertedTypes.Uint64},
		{"int8", schema.NewIntLogicalType(8 /* bitWidth */, true /* signed */), schema.ConvertedTypes.Int8},
		{"int16", schema.NewIntLogicalType(16 /* bitWidth */, true /* signed */), schema.ConvertedTypes.Int16},
		{"int32", schema.NewIntLogicalType(32 /* bitWidth */, true /* signed */), schema.ConvertedTypes.Int32},
		{"int64", schema.NewIntLogicalType(64 /* bitWidth */, true /* signed */), schema.ConvertedTypes.Int64},
		{"json", schema.JSONLogicalType{}, schema.ConvertedTypes.JSON},
		{"bson", schema.BSONLogicalType{}, schema.ConvertedTypes.BSON},
		{"interval", schema.IntervalLogicalType{}, schema.ConvertedTypes.Interval},
		{"none", schema.NoLogicalType{}, schema.ConvertedTypes.None},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.True(t, tt.logical.IsValid())
			converted, decimalMeta := tt.logical.ToConvertedType()
			assert.Equal(t, tt.expectConverted, converted)
			assert.False(t, decimalMeta.IsSet)
			assert.True(t, tt.logical.IsCompatible(converted, decimalMeta))
			assert.False(t, tt.logical.IsCompatible(converted, schema.DecimalMetadata{IsSet: true, Precision: 1, Scale: 1}))
			reconstruct := converted.ToLogicalType(decimalMeta)
			assert.True(t, reconstruct.IsValid())
			assert.True(t, reconstruct.Equals(tt.logical))
		})
	}

	var (
		orig          schema.LogicalType
		converted     schema.ConvertedType
		convertedMeta schema.DecimalMetadata
	)

	orig = schema.NewDecimalLogicalType(6 /* precision */, 2 /* scale */)
	converted, convertedMeta = orig.ToConvertedType()
	assert.True(t, orig.IsValid())
	assert.Equal(t, schema.ConvertedTypes.Decimal, converted)
	assert.True(t, convertedMeta.IsSet)
	assert.EqualValues(t, 6, convertedMeta.Precision)
	assert.EqualValues(t, 2, convertedMeta.Scale)
	assert.True(t, orig.IsCompatible(converted, convertedMeta))
	reconstruct := converted.ToLogicalType(convertedMeta)
	assert.True(t, reconstruct.IsValid())
	assert.True(t, reconstruct.Equals(orig))

	orig = schema.UnknownLogicalType{}
	converted, convertedMeta = orig.ToConvertedType()
	assert.False(t, orig.IsValid())
	assert.Equal(t, schema.ConvertedTypes.NA, converted)
	assert.False(t, convertedMeta.IsSet)
	assert.True(t, orig.IsCompatible(converted, convertedMeta))
	reconstruct = converted.ToLogicalType(convertedMeta)
	assert.False(t, reconstruct.IsValid())
	assert.True(t, reconstruct.Equals(orig))
}

func TestNewTypeIncompatibility(t *testing.T) {
	tests := []struct {
		name     string
		logical  schema.LogicalType
		expected schema.LogicalType
	}{
		{"uuid", schema.UUIDLogicalType{}, schema.UUIDLogicalType{}},
		{"null", schema.NullLogicalType{}, schema.NullLogicalType{}},
		{"not-utc-time_milli", schema.NewTimeLogicalType(false /* adjutedToUTC */, schema.TimeUnitMillis), &schema.TimeLogicalType{}},
		{"not-utc-time-micro", schema.NewTimeLogicalType(false /* adjutedToUTC */, schema.TimeUnitMicros), &schema.TimeLogicalType{}},
		{"not-utc-time-nano", schema.NewTimeLogicalType(false /* adjutedToUTC */, schema.TimeUnitNanos), &schema.TimeLogicalType{}},
		{"utc-time-nano", schema.NewTimeLogicalType(true /* adjutedToUTC */, schema.TimeUnitNanos), &schema.TimeLogicalType{}},
		{"not-utc-timestamp-nano", schema.NewTimestampLogicalType(false /* adjutedToUTC */, schema.TimeUnitNanos), &schema.TimestampLogicalType{}},
		{"utc-timestamp-nano", schema.NewTimestampLogicalType(true /* adjutedToUTC */, schema.TimeUnitNanos), &schema.TimestampLogicalType{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.IsType(t, tt.expected, tt.logical)
			assert.True(t, tt.logical.IsValid())
			converted, meta := tt.logical.ToConvertedType()
			assert.Equal(t, schema.ConvertedTypes.None, converted)
			assert.False(t, meta.IsSet)
		})
	}
}

func TestFactoryPanic(t *testing.T) {
	tests := []struct {
		name string
		f    func()
	}{
		{"invalid TimeUnit", func() { schema.NewTimeLogicalType(true /* adjutedToUTC */, schema.TimeUnitUnknown) }},
		{"invalid timestamp unit", func() { schema.NewTimestampLogicalType(true /* adjutedToUTC */, schema.TimeUnitUnknown) }},
		{"negative bitwidth", func() { schema.NewIntLogicalType(-1 /* bitWidth */, false /* signed */) }},
		{"zero bitwidth", func() { schema.NewIntLogicalType(0 /* bitWidth */, false /* signed */) }},
		{"bitwidth one", func() { schema.NewIntLogicalType(1 /* bitWidth */, false /* signed */) }},
		{"invalid bitwidth", func() { schema.NewIntLogicalType(65 /* bitWidth */, false /* signed */) }},
		{"negative precision", func() { schema.NewDecimalLogicalType(-1 /* precision */, 0 /* scale */) }},
		{"zero precision", func() { schema.NewDecimalLogicalType(0 /* precision */, 0 /* scale */) }},
		{"negative scale", func() { schema.NewDecimalLogicalType(10 /* precision */, -1 /* scale */) }},
		{"invalid scale", func() { schema.NewDecimalLogicalType(10 /* precision */, 11 /* scale */) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Panics(t, tt.f)
		})
	}
}

func TestLogicalTypeProperties(t *testing.T) {
	tests := []struct {
		name       string
		logical    schema.LogicalType
		nested     bool
		serialized bool
		valid      bool
	}{
		{"string", schema.StringLogicalType{}, false, true, true},
		{"map", schema.MapLogicalType{}, true, true, true},
		{"list", schema.NewListLogicalType(), true, true, true},
		{"enum", schema.EnumLogicalType{}, false, true, true},
		{"decimal", schema.NewDecimalLogicalType(16 /* precision */, 6 /* scale */), false, true, true},
		{"date", schema.DateLogicalType{}, false, true, true},
		{"time", schema.NewTimeLogicalType(true /* adjustedToUTC */, schema.TimeUnitMicros), false, true, true},
		{"timestamp", schema.NewTimestampLogicalType(true /* adjustedToUTC */, schema.TimeUnitMicros), false, true, true},
		{"interval", schema.IntervalLogicalType{}, false, true, true},
		{"uint8", schema.NewIntLogicalType(8 /* bitWidth */, false /* signed */), false, true, true},
		{"int64", schema.NewIntLogicalType(64 /* bitWidth */, true /* signed */), false, true, true},
		{"null", schema.NullLogicalType{}, false, true, true},
		{"json", schema.JSONLogicalType{}, false, true, true},
		{"bson", schema.BSONLogicalType{}, false, true, true},
		{"uuid", schema.UUIDLogicalType{}, false, true, true},
		{"nological", schema.NoLogicalType{}, false, false, true},
		{"unknown", schema.UnknownLogicalType{}, false, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.True(t, tt.nested == tt.logical.IsNested())
			assert.True(t, tt.serialized == tt.logical.IsSerialized())
			assert.True(t, tt.valid == tt.logical.IsValid())
		})
	}
}

var physicalTypeList = []parquet.Type{
	parquet.Types.Boolean,
	parquet.Types.Int32,
	parquet.Types.Int64,
	parquet.Types.Int96,
	parquet.Types.Float,
	parquet.Types.Double,
	parquet.Types.ByteArray,
	parquet.Types.FixedLenByteArray,
}

func TestLogicalSingleTypeApplicability(t *testing.T) {
	tests := []struct {
		name       string
		logical    schema.LogicalType
		applicable parquet.Type
	}{
		{"string", schema.StringLogicalType{}, parquet.Types.ByteArray},
		{"enum", schema.EnumLogicalType{}, parquet.Types.ByteArray},
		{"date", schema.DateLogicalType{}, parquet.Types.Int32},
		{"timemilli", schema.NewTimeLogicalType(true /* adjustedToUTC */, schema.TimeUnitMillis), parquet.Types.Int32},
		{"timemicro", schema.NewTimeLogicalType(true /* adjustedToUTC */, schema.TimeUnitMicros), parquet.Types.Int64},
		{"timenano", schema.NewTimeLogicalType(true /* adjustedToUTC */, schema.TimeUnitNanos), parquet.Types.Int64},
		{"timestampmilli", schema.NewTimestampLogicalType(true /* adjustedToUTC */, schema.TimeUnitMillis), parquet.Types.Int64},
		{"timestampmicro", schema.NewTimestampLogicalType(true /* adjustedToUTC */, schema.TimeUnitMicros), parquet.Types.Int64},
		{"timestampnanos", schema.NewTimestampLogicalType(true /* adjustedToUTC */, schema.TimeUnitNanos), parquet.Types.Int64},
		{"uint8", schema.NewIntLogicalType(8 /* bitWidth */, false /* signed */), parquet.Types.Int32},
		{"uint16", schema.NewIntLogicalType(16 /* bitWidth */, false /* signed */), parquet.Types.Int32},
		{"uint32", schema.NewIntLogicalType(32 /* bitWidth */, false /* signed */), parquet.Types.Int32},
		{"uint64", schema.NewIntLogicalType(64 /* bitWidth */, false /* signed */), parquet.Types.Int64},
		{"int8", schema.NewIntLogicalType(8 /* bitWidth */, true /* signed */), parquet.Types.Int32},
		{"int16", schema.NewIntLogicalType(16 /* bitWidth */, true /* signed */), parquet.Types.Int32},
		{"int32", schema.NewIntLogicalType(32 /* bitWidth */, true /* signed */), parquet.Types.Int32},
		{"int64", schema.NewIntLogicalType(64 /* bitWidth */, true /* signed */), parquet.Types.Int64},
		{"json", schema.JSONLogicalType{}, parquet.Types.ByteArray},
		{"bson", schema.BSONLogicalType{}, parquet.Types.ByteArray},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, typ := range physicalTypeList {
				if typ == tt.applicable {
					assert.True(t, tt.logical.IsApplicable(typ, -1))
				} else {
					assert.False(t, tt.logical.IsApplicable(typ, -1))
				}
			}
		})
	}
}

func TestLogicalNoTypeApplicability(t *testing.T) {
	tests := []struct {
		name    string
		logical schema.LogicalType
	}{
		{"map", schema.MapLogicalType{}},
		{"list", schema.NewListLogicalType()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, typ := range physicalTypeList {
				assert.False(t, tt.logical.IsApplicable(typ, -1))
			}
		})
	}
}

func TestLogicalUniversalTypeApplicability(t *testing.T) {
	tests := []struct {
		name    string
		logical schema.LogicalType
	}{
		{"null", schema.NullLogicalType{}},
		{"none", schema.NoLogicalType{}},
		{"unknown", schema.UnknownLogicalType{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, typ := range physicalTypeList {
				assert.True(t, tt.logical.IsApplicable(typ, -1))
			}
		})
	}
}

func TestLogicalInapplicableTypes(t *testing.T) {
	tests := []struct {
		name string
		typ  parquet.Type
		len  int32
	}{
		{"fixed 8", parquet.Types.FixedLenByteArray, 8},
		{"fixed 20", parquet.Types.FixedLenByteArray, 20},
		{"bool", parquet.Types.Boolean, -1},
		{"int32", parquet.Types.Int32, -1},
		{"int64", parquet.Types.Int64, -1},
		{"int96", parquet.Types.Int96, -1},
		{"float", parquet.Types.Float, -1},
		{"double", parquet.Types.Double, -1},
		{"bytearray", parquet.Types.ByteArray, -1},
	}

	var logical schema.LogicalType

	logical = schema.IntervalLogicalType{}
	assert.True(t, logical.IsApplicable(parquet.Types.FixedLenByteArray, 12))
	for _, tt := range tests {
		t.Run("interval "+tt.name, func(t *testing.T) {
			assert.False(t, logical.IsApplicable(tt.typ, tt.len))
		})
	}

	logical = schema.UUIDLogicalType{}
	assert.True(t, logical.IsApplicable(parquet.Types.FixedLenByteArray, 16))
	for _, tt := range tests {
		t.Run("uuid "+tt.name, func(t *testing.T) {
			assert.False(t, logical.IsApplicable(tt.typ, tt.len))
		})
	}
}

func TestDecimalLogicalTypeApplicability(t *testing.T) {
	const scale = 0
	var logical schema.LogicalType
	for prec := int32(1); prec <= 9; prec++ {
		logical = schema.NewDecimalLogicalType(prec, scale)
		assert.Truef(t, logical.IsApplicable(parquet.Types.Int32, -1), "prec: %d", prec)
	}

	logical = schema.NewDecimalLogicalType(10 /* precision */, scale)
	assert.False(t, logical.IsApplicable(parquet.Types.Int32, -1))

	for prec := int32(1); prec <= 18; prec++ {
		logical = schema.NewDecimalLogicalType(prec, scale)
		assert.Truef(t, logical.IsApplicable(parquet.Types.Int64, -1), "prec: %d", prec)
	}

	logical = schema.NewDecimalLogicalType(19, scale)
	assert.False(t, logical.IsApplicable(parquet.Types.Int64, 0))

	for prec := int32(1); prec <= 36; prec++ {
		logical = schema.NewDecimalLogicalType(prec, scale)
		assert.Truef(t, logical.IsApplicable(parquet.Types.ByteArray, 0), "prec: %d", prec)
	}

	tests := []struct {
		physicalLen    int32
		precisionLimit int32
	}{
		{1, 2}, {2, 4}, {3, 6}, {4, 9}, {8, 18}, {10, 23}, {16, 38}, {20, 47}, {32, 76},
	}
	for _, tt := range tests {
		var prec int32
		for prec = 1; prec <= tt.precisionLimit; prec++ {
			logical = schema.NewDecimalLogicalType(prec, 0)
			assert.Truef(t, logical.IsApplicable(parquet.Types.FixedLenByteArray, tt.physicalLen), "prec: %d, len: %d", prec, tt.physicalLen)
		}
		logical = schema.NewDecimalLogicalType(prec, 0)
		assert.Falsef(t, logical.IsApplicable(parquet.Types.FixedLenByteArray, tt.physicalLen), "prec: %d, len: %d", prec, tt.physicalLen)
	}

	assert.False(t, schema.NewDecimalLogicalType(16, 6).IsApplicable(parquet.Types.Boolean, 0))
	assert.False(t, schema.NewDecimalLogicalType(16, 6).IsApplicable(parquet.Types.Float, 0))
	assert.False(t, schema.NewDecimalLogicalType(16, 6).IsApplicable(parquet.Types.Double, 0))
}

func TestLogicalTypeRepresentation(t *testing.T) {
	tests := []struct {
		name     string
		logical  schema.LogicalType
		expected string
		expjson  string
	}{
		{"unknown", schema.UnknownLogicalType{}, "Unknown", `{"Type": "Unknown"}`},
		{"string", schema.StringLogicalType{}, "String", `{"Type": "String"}`},
		{"map", schema.MapLogicalType{}, "Map", `{"Type": "Map"}`},
		{"list", schema.NewListLogicalType(), "List", `{"Type": "List"}`},
		{"enum", schema.EnumLogicalType{}, "Enum", `{"Type": "Enum"}`},
		{"decimal 10 4", schema.NewDecimalLogicalType(10 /* precision */, 4 /* scale */), "Decimal(precision=10, scale=4)", `{"Type": "Decimal", "precision": 10, "scale": 4}`},
		{"decimal 10 0", schema.NewDecimalLogicalType(10 /* precision */, 0 /* scale */), "Decimal(precision=10, scale=0)", `{"Type": "Decimal", "precision": 10, "scale": 0}`},
		{"date", schema.DateLogicalType{}, "Date", `{"Type": "Date"}`},
		{"time milli", schema.NewTimeLogicalType(true /* adjustedToUTC */, schema.TimeUnitMillis), "Time(isAdjustedToUTC=true, timeUnit=milliseconds)", `{"Type": "Time", "isAdjustedToUTC": true, "timeUnit": "milliseconds"}`},
		{"time micro", schema.NewTimeLogicalType(true /* adjustedToUTC */, schema.TimeUnitMicros), "Time(isAdjustedToUTC=true, timeUnit=microseconds)", `{"Type": "Time", "isAdjustedToUTC": true, "timeUnit": "microseconds"}`},
		{"time nano", schema.NewTimeLogicalType(true /* adjustedToUTC */, schema.TimeUnitNanos), "Time(isAdjustedToUTC=true, timeUnit=nanoseconds)", `{"Type": "Time", "isAdjustedToUTC": true, "timeUnit": "nanoseconds"}`},
		{"time notutc milli", schema.NewTimeLogicalType(false /* adjustedToUTC */, schema.TimeUnitMillis), "Time(isAdjustedToUTC=false, timeUnit=milliseconds)", `{"Type": "Time", "isAdjustedToUTC": false, "timeUnit": "milliseconds"}`},
		{"time notutc micro", schema.NewTimeLogicalType(false /* adjustedToUTC */, schema.TimeUnitMicros), "Time(isAdjustedToUTC=false, timeUnit=microseconds)", `{"Type": "Time", "isAdjustedToUTC": false, "timeUnit": "microseconds"}`},
		{"time notutc nano", schema.NewTimeLogicalType(false /* adjustedToUTC */, schema.TimeUnitNanos), "Time(isAdjustedToUTC=false, timeUnit=nanoseconds)", `{"Type": "Time", "isAdjustedToUTC": false, "timeUnit": "nanoseconds"}`},
		{"timestamp milli", schema.NewTimestampLogicalType(true /* adjustedToUTC */, schema.TimeUnitMillis), "Timestamp(isAdjustedToUTC=true, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false)", `{"Type": "Timestamp", "isAdjustedToUTC": true, "timeUnit": "milliseconds", "is_from_converted_type": false, "force_set_converted_type": false}`},
		{"timestamp micro", schema.NewTimestampLogicalType(true /* adjustedToUTC */, schema.TimeUnitMicros), "Timestamp(isAdjustedToUTC=true, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false)", `{"Type": "Timestamp", "isAdjustedToUTC": true, "timeUnit": "microseconds", "is_from_converted_type": false, "force_set_converted_type": false}`},
		{"timestamp nano", schema.NewTimestampLogicalType(true /* adjustedToUTC */, schema.TimeUnitNanos), "Timestamp(isAdjustedToUTC=true, timeUnit=nanoseconds, is_from_converted_type=false, force_set_converted_type=false)", `{"Type": "Timestamp", "isAdjustedToUTC": true, "timeUnit": "nanoseconds", "is_from_converted_type": false, "force_set_converted_type": false}`},
		{"timestamp notutc milli", schema.NewTimestampLogicalType(false /* adjustedToUTC */, schema.TimeUnitMillis), "Timestamp(isAdjustedToUTC=false, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false)", `{"Type": "Timestamp", "isAdjustedToUTC": false, "timeUnit": "milliseconds", "is_from_converted_type": false, "force_set_converted_type": false}`},
		{"timestamp notutc micro", schema.NewTimestampLogicalType(false /* adjustedToUTC */, schema.TimeUnitMicros), "Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false)", `{"Type": "Timestamp", "isAdjustedToUTC": false, "timeUnit": "microseconds", "is_from_converted_type": false, "force_set_converted_type": false}`},
		{"timestamp notutc nano", schema.NewTimestampLogicalType(false /* adjustedToUTC */, schema.TimeUnitNanos), "Timestamp(isAdjustedToUTC=false, timeUnit=nanoseconds, is_from_converted_type=false, force_set_converted_type=false)", `{"Type": "Timestamp", "isAdjustedToUTC": false, "timeUnit": "nanoseconds", "is_from_converted_type": false, "force_set_converted_type": false}`},
		{"interval", schema.IntervalLogicalType{}, "Interval", `{"Type": "Interval"}`},
		{"uint8", schema.NewIntLogicalType(8 /* bitWidth */, false /* signed */), "Int(bitWidth=8, isSigned=false)", `{"Type": "Int", "bitWidth": 8, "isSigned": false}`},
		{"uint16", schema.NewIntLogicalType(16 /* bitWidth */, false /* signed */), "Int(bitWidth=16, isSigned=false)", `{"Type": "Int", "bitWidth": 16, "isSigned": false}`},
		{"uint32", schema.NewIntLogicalType(32 /* bitWidth */, false /* signed */), "Int(bitWidth=32, isSigned=false)", `{"Type": "Int", "bitWidth": 32, "isSigned": false}`},
		{"uint64", schema.NewIntLogicalType(64 /* bitWidth */, false /* signed */), "Int(bitWidth=64, isSigned=false)", `{"Type": "Int", "bitWidth": 64, "isSigned": false}`},
		{"int8", schema.NewIntLogicalType(8 /* bitWidth */, true /* signed */), "Int(bitWidth=8, isSigned=true)", `{"Type": "Int", "bitWidth": 8, "isSigned": true}`},
		{"int16", schema.NewIntLogicalType(16 /* bitWidth */, true /* signed */), "Int(bitWidth=16, isSigned=true)", `{"Type": "Int", "bitWidth": 16, "isSigned": true}`},
		{"int32", schema.NewIntLogicalType(32 /* bitWidth */, true /* signed */), "Int(bitWidth=32, isSigned=true)", `{"Type": "Int", "bitWidth": 32, "isSigned": true}`},
		{"int64", schema.NewIntLogicalType(64 /* bitWidth */, true /* signed */), "Int(bitWidth=64, isSigned=true)", `{"Type": "Int", "bitWidth": 64, "isSigned": true}`},
		{"null", schema.NullLogicalType{}, "Null", `{"Type": "Null"}`},
		{"json", schema.JSONLogicalType{}, "JSON", `{"Type": "JSON"}`},
		{"bson", schema.BSONLogicalType{}, "BSON", `{"Type": "BSON"}`},
		{"uuid", schema.UUIDLogicalType{}, "UUID", `{"Type": "UUID"}`},
		{"none", schema.NoLogicalType{}, "None", `{"Type": "None"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.logical.String())
			out, err := json.Marshal(tt.logical)
			assert.NoError(t, err)
			assert.JSONEq(t, tt.expjson, string(out))
		})
	}
}

func TestLogicalTypeSortOrder(t *testing.T) {
	tests := []struct {
		name    string
		logical schema.LogicalType
		order   schema.SortOrder
	}{
		{"unknown", schema.UnknownLogicalType{}, schema.SortUNKNOWN},
		{"string", schema.StringLogicalType{}, schema.SortUNSIGNED},
		{"map", schema.MapLogicalType{}, schema.SortUNKNOWN},
		{"list", schema.NewListLogicalType(), schema.SortUNKNOWN},
		{"enum", schema.EnumLogicalType{}, schema.SortUNSIGNED},
		{"decimal", schema.NewDecimalLogicalType(8 /* precision */, 2 /* scale */), schema.SortSIGNED},
		{"date", schema.DateLogicalType{}, schema.SortSIGNED},
		{"time utc milli", schema.NewTimeLogicalType(true /* adjustedToUTC */, schema.TimeUnitMillis), schema.SortSIGNED},
		{"time utc micros", schema.NewTimeLogicalType(true /* adjustedToUTC */, schema.TimeUnitMicros), schema.SortSIGNED},
		{"time utc nanos", schema.NewTimeLogicalType(true /* adjustedToUTC */, schema.TimeUnitNanos), schema.SortSIGNED},
		{"time not utc milli", schema.NewTimeLogicalType(false /* adjustedToUTC */, schema.TimeUnitMillis), schema.SortSIGNED},
		{"time not utc micros", schema.NewTimeLogicalType(false /* adjustedToUTC */, schema.TimeUnitMicros), schema.SortSIGNED},
		{"time not utc nanos", schema.NewTimeLogicalType(false /* adjustedToUTC */, schema.TimeUnitNanos), schema.SortSIGNED},
		{"interval", schema.IntervalLogicalType{}, schema.SortUNKNOWN},
		{"uint8", schema.NewIntLogicalType(8 /* bitWidth */, false /* signed */), schema.SortUNSIGNED},
		{"uint16", schema.NewIntLogicalType(16 /* bitWidth */, false /* signed */), schema.SortUNSIGNED},
		{"uint32", schema.NewIntLogicalType(32 /* bitWidth */, false /* signed */), schema.SortUNSIGNED},
		{"uint64", schema.NewIntLogicalType(64 /* bitWidth */, false /* signed */), schema.SortUNSIGNED},
		{"int8", schema.NewIntLogicalType(8 /* bitWidth */, true /* signed */), schema.SortSIGNED},
		{"int16", schema.NewIntLogicalType(16 /* bitWidth */, true /* signed */), schema.SortSIGNED},
		{"int32", schema.NewIntLogicalType(32 /* bitWidth */, true /* signed */), schema.SortSIGNED},
		{"int64", schema.NewIntLogicalType(64 /* bitWidth */, true /* signed */), schema.SortSIGNED},
		{"null", schema.NullLogicalType{}, schema.SortUNKNOWN},
		{"json", schema.JSONLogicalType{}, schema.SortUNSIGNED},
		{"bson", schema.BSONLogicalType{}, schema.SortUNSIGNED},
		{"uuid", schema.UUIDLogicalType{}, schema.SortUNSIGNED},
		{"none", schema.NoLogicalType{}, schema.SortUNKNOWN},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.order, tt.logical.SortOrder())
		})
	}
}

func TestNodeFactoryEquivalences(t *testing.T) {
	tests := []struct {
		name        string
		logical     schema.LogicalType
		converted   schema.ConvertedType
		typ         parquet.Type
		physicalLen int
		precision   int
		scale       int
	}{
		{"string", schema.StringLogicalType{}, schema.ConvertedTypes.UTF8, parquet.Types.ByteArray, -1, -1, -1},
		{"enum", schema.EnumLogicalType{}, schema.ConvertedTypes.Enum, parquet.Types.ByteArray, -1, -1, -1},
		{"decimal", schema.NewDecimalLogicalType(16 /* precision */, 6 /* scale */), schema.ConvertedTypes.Decimal, parquet.Types.Int64, -1, 16, 6},
		{"date", schema.DateLogicalType{}, schema.ConvertedTypes.Date, parquet.Types.Int32, -1, -1, -1},
		{"time millis", schema.NewTimeLogicalType(true /* adjustedToUTC */, schema.TimeUnitMillis), schema.ConvertedTypes.TimeMillis, parquet.Types.Int32, -1, -1, -1},
		{"time micros", schema.NewTimeLogicalType(true /* adjustedToUTC */, schema.TimeUnitMicros), schema.ConvertedTypes.TimeMicros, parquet.Types.Int64, -1, -1, -1},
		{"timestamp millis", schema.NewTimestampLogicalType(true /* adjustedToUTC */, schema.TimeUnitMillis), schema.ConvertedTypes.TimestampMillis, parquet.Types.Int64, -1, -1, -1},
		{"timestamp micros", schema.NewTimestampLogicalType(true /* adjustedToUTC */, schema.TimeUnitMicros), schema.ConvertedTypes.TimestampMicros, parquet.Types.Int64, -1, -1, -1},
		{"interval", schema.IntervalLogicalType{}, schema.ConvertedTypes.Interval, parquet.Types.FixedLenByteArray, 12, -1, -1},
		{"uint8", schema.NewIntLogicalType(8 /* bitWidth */, false /* signed */), schema.ConvertedTypes.Uint8, parquet.Types.Int32, -1, -1, -1},
		{"int8", schema.NewIntLogicalType(8 /* bitWidth */, true /* signed */), schema.ConvertedTypes.Int8, parquet.Types.Int32, -1, -1, -1},
		{"uint16", schema.NewIntLogicalType(16 /* bitWidth */, false /* signed */), schema.ConvertedTypes.Uint16, parquet.Types.Int32, -1, -1, -1},
		{"int16", schema.NewIntLogicalType(16 /* bitWidth */, true /* signed */), schema.ConvertedTypes.Int16, parquet.Types.Int32, -1, -1, -1},
		{"uint32", schema.NewIntLogicalType(32 /* bitWidth */, false /* signed */), schema.ConvertedTypes.Uint32, parquet.Types.Int32, -1, -1, -1},
		{"int32", schema.NewIntLogicalType(32 /* bitWidth */, true /* signed */), schema.ConvertedTypes.Int32, parquet.Types.Int32, -1, -1, -1},
		{"uint64", schema.NewIntLogicalType(64 /* bitWidth */, false /* signed */), schema.ConvertedTypes.Uint64, parquet.Types.Int64, -1, -1, -1},
		{"int64", schema.NewIntLogicalType(64 /* bitWidth */, true /* signed */), schema.ConvertedTypes.Int64, parquet.Types.Int64, -1, -1, -1},
		{"json", schema.JSONLogicalType{}, schema.ConvertedTypes.JSON, parquet.Types.ByteArray, -1, -1, -1},
		{"bson", schema.BSONLogicalType{}, schema.ConvertedTypes.BSON, parquet.Types.ByteArray, -1, -1, -1},
		{"none", schema.NoLogicalType{}, schema.ConvertedTypes.None, parquet.Types.Int64, -1, -1, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name := "something"
			repetition := parquet.Repetitions.Required

			fromConverted := schema.MustPrimitive(schema.NewPrimitiveNodeConverted(name, repetition, tt.typ, tt.converted, tt.physicalLen, tt.precision, tt.scale, -1 /* fieldID */))
			fromLogical := schema.MustPrimitive(schema.NewPrimitiveNodeLogical(name, repetition, tt.logical, tt.typ, tt.physicalLen, -1 /* fieldID */))
			assert.True(t, fromConverted.Equals(fromLogical))
		})
	}

	rep := parquet.Repetitions.Optional
	fromConverted, err := schema.NewGroupNodeConverted("map" /* name */, rep, []schema.Node{}, schema.ConvertedTypes.Map, -1 /* fieldID */)
	assert.NoError(t, err)

	fromLogical, err := schema.NewGroupNodeLogical("map" /* name */, rep, []schema.Node{}, schema.MapLogicalType{}, -1 /* fieldID */)
	assert.NoError(t, err)
	assert.True(t, fromConverted.Equals(fromLogical))

	fromConverted, err = schema.NewGroupNodeConverted("list" /* name */, rep, []schema.Node{}, schema.ConvertedTypes.List, -1 /* fieldID */)
	assert.NoError(t, err)

	fromLogical, err = schema.NewGroupNodeLogical("list" /* name */, rep, []schema.Node{}, schema.NewListLogicalType(), -1 /* fieldID */)
	assert.NoError(t, err)
	assert.True(t, fromConverted.Equals(fromLogical))
}
