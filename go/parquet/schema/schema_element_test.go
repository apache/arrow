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

package schema

import (
	"testing"

	"github.com/apache/arrow/go/parquet"
	format "github.com/apache/arrow/go/parquet/internal/gen-go/parquet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type schemaElementConstruction struct {
	node            Node
	element         *format.SchemaElement
	name            string
	expectConverted bool
	converted       ConvertedType
	expectLogical   bool
	checkLogical    func(*format.SchemaElement) bool
}

type decimalSchemaElementConstruction struct {
	schemaElementConstruction
	precision int
	scale     int
}

type temporalSchemaElementConstruction struct {
	schemaElementConstruction
	adjusted bool
	unit     TimeUnitType
	getUnit  func(*format.SchemaElement) *format.TimeUnit
}

type intSchemaElementConstruction struct {
	schemaElementConstruction
	width  int8
	signed bool
}

type legacySchemaElementConstructArgs struct {
	name            string
	physical        parquet.Type
	len             int
	expectConverted bool
	converted       ConvertedType
	expectLogical   bool
	checkLogical    func(*format.SchemaElement) bool
}

type schemaElementConstructArgs struct {
	name            string
	logical         LogicalType
	physical        parquet.Type
	len             int
	expectConverted bool
	converted       ConvertedType
	expectLogical   bool
	checkLogical    func(*format.SchemaElement) bool
}
type SchemaElementConstructionSuite struct {
	suite.Suite
}

func (s *SchemaElementConstructionSuite) reconstruct(c schemaElementConstructArgs) *schemaElementConstruction {
	ret := &schemaElementConstruction{
		node:            NewPrimitiveNodeLogical(c.name, parquet.Repetitions.Required, c.logical, c.physical, c.len, -1),
		name:            c.name,
		expectConverted: c.expectConverted,
		converted:       c.converted,
		expectLogical:   c.expectLogical,
		checkLogical:    c.checkLogical,
	}
	ret.element = ret.node.toThrift()
	return ret
}

func (s *SchemaElementConstructionSuite) legacyReconstruct(c legacySchemaElementConstructArgs) *schemaElementConstruction {
	ret := &schemaElementConstruction{
		node:            NewPrimitiveNodeConverted(c.name, parquet.Repetitions.Required, c.physical, c.converted, c.len, 0, 0, -1),
		name:            c.name,
		expectConverted: c.expectConverted,
		converted:       c.converted,
		expectLogical:   c.expectLogical,
		checkLogical:    c.checkLogical,
	}
	ret.element = ret.node.toThrift()
	return ret
}

func (s *SchemaElementConstructionSuite) inspect(c *schemaElementConstruction) {
	if c.expectConverted {
		s.True(c.element.IsSetConvertedType())
		s.Equal(c.converted, ConvertedType(*c.element.ConvertedType))
	} else {
		s.False(c.element.IsSetConvertedType())
	}
	if c.expectLogical {
		s.True(c.element.IsSetLogicalType())
		s.True(c.checkLogical(c.element))
	} else {
		s.False(c.element.IsSetLogicalType())
	}
}

func (s *SchemaElementConstructionSuite) TestSimple() {
	checkNone := func(*format.SchemaElement) bool { return true }

	tests := []struct {
		name   string
		args   *schemaElementConstructArgs
		legacy *legacySchemaElementConstructArgs
	}{
		{"string", &schemaElementConstructArgs{
			"string", StringLogicalType{}, parquet.Types.ByteArray, -1, true, ConvertedTypes.UTF8, true,
			func(e *format.SchemaElement) bool { return e.LogicalType.IsSetSTRING() },
		}, nil},
		{"enum", &schemaElementConstructArgs{
			"enum", EnumLogicalType{}, parquet.Types.ByteArray, -1, true, ConvertedTypes.Enum, true,
			func(e *format.SchemaElement) bool { return e.LogicalType.IsSetENUM() },
		}, nil},
		{"date", &schemaElementConstructArgs{
			"date", DateLogicalType{}, parquet.Types.Int32, -1, true, ConvertedTypes.Date, true,
			func(e *format.SchemaElement) bool { return e.LogicalType.IsSetDATE() },
		}, nil},
		{"interval", &schemaElementConstructArgs{
			"interval", IntervalLogicalType{}, parquet.Types.FixedLenByteArray, 12, true, ConvertedTypes.Interval, false,
			checkNone,
		}, nil},
		{"null", &schemaElementConstructArgs{
			"null", NullLogicalType{}, parquet.Types.Double, -1, false, ConvertedTypes.NA, true,
			func(e *format.SchemaElement) bool { return e.LogicalType.IsSetUNKNOWN() },
		}, nil},
		{"json", &schemaElementConstructArgs{
			"json", JSONLogicalType{}, parquet.Types.ByteArray, -1, true, ConvertedTypes.JSON, true,
			func(e *format.SchemaElement) bool { return e.LogicalType.IsSetJSON() },
		}, nil},
		{"bson", &schemaElementConstructArgs{
			"bson", BSONLogicalType{}, parquet.Types.ByteArray, -1, true, ConvertedTypes.BSON, true,
			func(e *format.SchemaElement) bool { return e.LogicalType.IsSetBSON() },
		}, nil},
		{"uuid", &schemaElementConstructArgs{
			"uuid", UUIDLogicalType{}, parquet.Types.FixedLenByteArray, 16, false, ConvertedTypes.NA, true,
			func(e *format.SchemaElement) bool { return e.LogicalType.IsSetUUID() },
		}, nil},
		{"none", &schemaElementConstructArgs{
			"none", NoLogicalType{}, parquet.Types.Int64, -1, false, ConvertedTypes.NA, false,
			checkNone,
		}, nil},
		{"unknown", &schemaElementConstructArgs{
			"unknown", UnknownLogicalType{}, parquet.Types.Int64, -1, true, ConvertedTypes.NA, false,
			checkNone,
		}, nil},
		{"timestamp_ms", nil, &legacySchemaElementConstructArgs{
			"timestamp_ms", parquet.Types.Int64, -1, true, ConvertedTypes.TimestampMillis, false, checkNone}},
		{"timestamp_us", nil, &legacySchemaElementConstructArgs{
			"timestamp_us", parquet.Types.Int64, -1, true, ConvertedTypes.TimestampMicros, false, checkNone}},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			var sc *schemaElementConstruction
			if tt.args != nil {
				sc = s.reconstruct(*tt.args)
			} else {
				sc = s.legacyReconstruct(*tt.legacy)
			}
			s.Equal(tt.name, sc.element.Name)
			s.inspect(sc)
		})
	}
}

func (s *SchemaElementConstructionSuite) reconstructDecimal(c schemaElementConstructArgs) *decimalSchemaElementConstruction {
	ret := s.reconstruct(c)
	dec := c.logical.(*DecimalLogicalType)
	return &decimalSchemaElementConstruction{*ret, int(dec.Precision()), int(dec.Scale())}
}

func (s *SchemaElementConstructionSuite) inspectDecimal(d *decimalSchemaElementConstruction) {
	s.inspect(&d.schemaElementConstruction)
	s.EqualValues(d.precision, d.element.GetPrecision())
	s.EqualValues(d.scale, d.element.GetScale())
	s.EqualValues(d.precision, d.element.LogicalType.DECIMAL.Precision)
	s.EqualValues(d.scale, d.element.LogicalType.DECIMAL.Scale)
}

func (s *SchemaElementConstructionSuite) TestDecimal() {
	checkDecimal := func(p *format.SchemaElement) bool { return p.LogicalType.IsSetDECIMAL() }

	tests := []schemaElementConstructArgs{
		{"decimal16_6", NewDecimalLogicalType(16, 6), parquet.Types.Int64, -1, true, ConvertedTypes.Decimal, true, checkDecimal},
		{"decimal1_0", NewDecimalLogicalType(1, 0), parquet.Types.Int32, -1, true, ConvertedTypes.Decimal, true, checkDecimal},
		{"decimal10", NewDecimalLogicalType(10, 0), parquet.Types.Int64, -1, true, ConvertedTypes.Decimal, true, checkDecimal},
		{"decimal11_11", NewDecimalLogicalType(11, 11), parquet.Types.Int64, -1, true, ConvertedTypes.Decimal, true, checkDecimal},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			d := s.reconstructDecimal(tt)
			s.Equal(tt.name, d.element.Name)
			s.inspectDecimal(d)
		})
	}
}

func (s *SchemaElementConstructionSuite) reconstructTemporal(c schemaElementConstructArgs, getUnit func(*format.SchemaElement) *format.TimeUnit) *temporalSchemaElementConstruction {
	base := s.reconstruct(c)
	t := c.logical.(TemporalLogicalType)
	return &temporalSchemaElementConstruction{
		*base,
		t.IsAdjustedToUTC(),
		t.TimeUnit(),
		getUnit,
	}
}

func (s *SchemaElementConstructionSuite) inspectTemporal(t *temporalSchemaElementConstruction) {
	s.inspect(&t.schemaElementConstruction)
	switch t.unit {
	case TimeUnitMillis:
		s.True(t.getUnit(t.element).IsSetMILLIS())
	case TimeUnitMicros:
		s.True(t.getUnit(t.element).IsSetMICROS())
	case TimeUnitNanos:
		s.True(t.getUnit(t.element).IsSetNANOS())
	case TimeUnitUnknown:
		fallthrough
	default:
		s.Fail("invalid time unit in test case")
	}
}

func (s *SchemaElementConstructionSuite) TestTemporal() {
	checkTime := func(p *format.SchemaElement) bool {
		return p.LogicalType.IsSetTIME()
	}
	checkTimestamp := func(p *format.SchemaElement) bool {
		return p.LogicalType.IsSetTIMESTAMP()
	}

	getTimeUnit := func(p *format.SchemaElement) *format.TimeUnit {
		return p.LogicalType.TIME.Unit
	}
	getTimestampUnit := func(p *format.SchemaElement) *format.TimeUnit {
		return p.LogicalType.TIMESTAMP.Unit
	}

	timeTests := []schemaElementConstructArgs{
		{"time_T_ms", NewTimeLogicalType(true, TimeUnitMillis), parquet.Types.Int32, -1, true, ConvertedTypes.TimeMillis, true, checkTime},
		{"time_F_ms", NewTimeLogicalType(false, TimeUnitMillis), parquet.Types.Int32, -1, false, ConvertedTypes.NA, true, checkTime},
		{"time_T_us", NewTimeLogicalType(true, TimeUnitMicros), parquet.Types.Int64, -1, true, ConvertedTypes.TimeMicros, true, checkTime},
		{"time_F_us", NewTimeLogicalType(false, TimeUnitMicros), parquet.Types.Int64, -1, false, ConvertedTypes.NA, true, checkTime},
		{"time_T_ns", NewTimeLogicalType(true, TimeUnitNanos), parquet.Types.Int64, -1, false, ConvertedTypes.NA, true, checkTime},
		{"time_F_ns", NewTimeLogicalType(false, TimeUnitNanos), parquet.Types.Int64, -1, false, ConvertedTypes.NA, true, checkTime},
	}
	timeStampTests := []schemaElementConstructArgs{
		{"timestamp_T_ms", NewTimestampLogicalType(true, TimeUnitMillis), parquet.Types.Int64, -1, true, ConvertedTypes.TimestampMillis, true, checkTimestamp},
		{"timestamp_F_ms", NewTimestampLogicalType(false, TimeUnitMillis), parquet.Types.Int64, -1, false, ConvertedTypes.NA, true, checkTimestamp},
		{"timestamp_F_ms_force", NewTimestampLogicalTypeForce(false, TimeUnitMillis), parquet.Types.Int64, -1, true, ConvertedTypes.TimestampMillis, true, checkTimestamp},
		{"timestamp_T_us", NewTimestampLogicalType(true, TimeUnitMicros), parquet.Types.Int64, -1, true, ConvertedTypes.TimestampMicros, true, checkTimestamp},
		{"timestamp_F_us", NewTimestampLogicalType(false, TimeUnitMicros), parquet.Types.Int64, -1, false, ConvertedTypes.NA, true, checkTimestamp},
		{"timestamp_F_us_force", NewTimestampLogicalTypeForce(false, TimeUnitMicros), parquet.Types.Int64, -1, true, ConvertedTypes.TimestampMicros, true, checkTimestamp},
		{"timestamp_T_ns", NewTimestampLogicalType(true, TimeUnitNanos), parquet.Types.Int64, -1, false, ConvertedTypes.NA, true, checkTimestamp},
		{"timestamp_F_ns", NewTimestampLogicalType(false, TimeUnitNanos), parquet.Types.Int64, -1, false, ConvertedTypes.NA, true, checkTimestamp},
	}

	for _, tt := range timeTests {
		s.Run(tt.name, func() {
			t := s.reconstructTemporal(tt, getTimeUnit)
			s.Equal(t.adjusted, t.element.LogicalType.TIME.IsAdjustedToUTC)
			s.inspectTemporal(t)
		})
	}
	for _, tt := range timeStampTests {
		s.Run(tt.name, func() {
			t := s.reconstructTemporal(tt, getTimestampUnit)
			s.Equal(t.adjusted, t.element.LogicalType.TIMESTAMP.IsAdjustedToUTC)
			s.inspectTemporal(t)
		})
	}
}

func (s *SchemaElementConstructionSuite) reconstructInteger(c schemaElementConstructArgs) *intSchemaElementConstruction {
	base := s.reconstruct(c)
	l := c.logical.(*IntLogicalType)
	return &intSchemaElementConstruction{
		*base,
		l.BitWidth(),
		l.IsSigned(),
	}
}

func (s *SchemaElementConstructionSuite) inspectInt(i *intSchemaElementConstruction) {
	s.inspect(&i.schemaElementConstruction)
	s.Equal(i.width, i.element.LogicalType.INTEGER.BitWidth)
	s.Equal(i.signed, i.element.LogicalType.INTEGER.IsSigned)
}

func (s *SchemaElementConstructionSuite) TestIntegerCases() {
	checkInt := func(p *format.SchemaElement) bool { return p.LogicalType.IsSetINTEGER() }

	tests := []schemaElementConstructArgs{
		{"uint8", NewIntLogicalType(8, false), parquet.Types.Int32, -1, true, ConvertedTypes.Uint8, true, checkInt},
		{"uint16", NewIntLogicalType(16, false), parquet.Types.Int32, -1, true, ConvertedTypes.Uint16, true, checkInt},
		{"uint32", NewIntLogicalType(32, false), parquet.Types.Int32, -1, true, ConvertedTypes.Uint32, true, checkInt},
		{"uint64", NewIntLogicalType(64, false), parquet.Types.Int64, -1, true, ConvertedTypes.Uint64, true, checkInt},
		{"int8", NewIntLogicalType(8, true), parquet.Types.Int32, -1, true, ConvertedTypes.Int8, true, checkInt},
		{"int16", NewIntLogicalType(16, true), parquet.Types.Int32, -1, true, ConvertedTypes.Int16, true, checkInt},
		{"int32", NewIntLogicalType(32, true), parquet.Types.Int32, -1, true, ConvertedTypes.Int32, true, checkInt},
		{"int64", NewIntLogicalType(64, true), parquet.Types.Int64, -1, true, ConvertedTypes.Int64, true, checkInt},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			t := s.reconstructInteger(tt)
			s.inspectInt(t)
		})
	}
}

func TestSchemaElementNestedSerialization(t *testing.T) {
	// confirm that the intermediate thrift objects created during node serialization
	// contain correct ConvertedType and ConvertedType information

	strNode := NewPrimitiveNodeLogical("string", parquet.Repetitions.Required, StringLogicalType{}, parquet.Types.ByteArray, -1, -1)
	dateNode := NewPrimitiveNodeLogical("date", parquet.Repetitions.Required, DateLogicalType{}, parquet.Types.Int32, -1, -1)
	jsonNode := NewPrimitiveNodeLogical("json", parquet.Repetitions.Required, JSONLogicalType{}, parquet.Types.ByteArray, -1, -1)
	uuidNode := NewPrimitiveNodeLogical("uuid", parquet.Repetitions.Required, UUIDLogicalType{}, parquet.Types.FixedLenByteArray, 16, -1)
	timestampNode := NewPrimitiveNodeLogical("timestamp", parquet.Repetitions.Required, NewTimestampLogicalType(false, TimeUnitNanos), parquet.Types.Int64, -1, -1)
	intNode := NewPrimitiveNodeLogical("int", parquet.Repetitions.Required, NewIntLogicalType(64, false), parquet.Types.Int64, -1, -1)
	decimalNode := NewPrimitiveNodeLogical("decimal", parquet.Repetitions.Required, NewDecimalLogicalType(16, 6), parquet.Types.Int64, -1, -1)
	listNode := NewGroupNodeLogical("list", parquet.Repetitions.Repeated, []Node{strNode, dateNode, jsonNode, uuidNode, timestampNode, intNode, decimalNode}, NewListLogicalType(), -1)

	listElems := ToThrift(listNode)
	assert.Equal(t, "list", listElems[0].Name)
	assert.True(t, listElems[0].IsSetConvertedType())
	assert.True(t, listElems[0].IsSetLogicalType())
	assert.Equal(t, format.ConvertedType(ConvertedTypes.List), listElems[0].GetConvertedType())
	assert.True(t, listElems[0].LogicalType.IsSetLIST())
	assert.True(t, listElems[1].LogicalType.IsSetSTRING())
	assert.True(t, listElems[2].LogicalType.IsSetDATE())
	assert.True(t, listElems[3].LogicalType.IsSetJSON())
	assert.True(t, listElems[4].LogicalType.IsSetUUID())
	assert.True(t, listElems[5].LogicalType.IsSetTIMESTAMP())
	assert.True(t, listElems[6].LogicalType.IsSetINTEGER())
	assert.True(t, listElems[7].LogicalType.IsSetDECIMAL())

	mapNode := NewGroupNodeLogical("map", parquet.Repetitions.Required, []Node{}, MapLogicalType{}, -1)
	mapElems := ToThrift(mapNode)
	assert.Equal(t, "map", mapElems[0].Name)
	assert.True(t, mapElems[0].IsSetConvertedType())
	assert.True(t, mapElems[0].IsSetLogicalType())
	assert.Equal(t, format.ConvertedType(ConvertedTypes.Map), mapElems[0].GetConvertedType())
	assert.True(t, mapElems[0].LogicalType.IsSetMAP())
}

func TestLogicalTypeSerializationRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		logical  LogicalType
		physical parquet.Type
		len      int
	}{
		{"string", StringLogicalType{}, parquet.Types.ByteArray, -1},
		{"enum", EnumLogicalType{}, parquet.Types.ByteArray, -1},
		{"decimal", NewDecimalLogicalType(16, 6), parquet.Types.Int64, -1},
		{"date", DateLogicalType{}, parquet.Types.Int32, -1},
		{"time_T_ms", NewTimeLogicalType(true, TimeUnitMillis), parquet.Types.Int32, -1},
		{"time_T_us", NewTimeLogicalType(true, TimeUnitMicros), parquet.Types.Int64, -1},
		{"time_T_ns", NewTimeLogicalType(true, TimeUnitNanos), parquet.Types.Int64, -1},
		{"time_F_ms", NewTimeLogicalType(false, TimeUnitMillis), parquet.Types.Int32, -1},
		{"time_F_us", NewTimeLogicalType(false, TimeUnitMicros), parquet.Types.Int64, -1},
		{"time_F_ns", NewTimeLogicalType(false, TimeUnitNanos), parquet.Types.Int64, -1},
		{"timestamp_T_ms", NewTimestampLogicalType(true, TimeUnitMillis), parquet.Types.Int64, -1},
		{"timestamp_T_us", NewTimestampLogicalType(true, TimeUnitMicros), parquet.Types.Int64, -1},
		{"timestamp_T_ns", NewTimestampLogicalType(true, TimeUnitNanos), parquet.Types.Int64, -1},
		{"timestamp_F_ms", NewTimestampLogicalType(false, TimeUnitMillis), parquet.Types.Int64, -1},
		{"timestamp_F_us", NewTimestampLogicalType(false, TimeUnitMicros), parquet.Types.Int64, -1},
		{"timestamp_F_ns", NewTimestampLogicalType(false, TimeUnitNanos), parquet.Types.Int64, -1},
		{"interval", IntervalLogicalType{}, parquet.Types.FixedLenByteArray, 12},
		{"uint8", NewIntLogicalType(8, false), parquet.Types.Int32, -1},
		{"uint16", NewIntLogicalType(16, false), parquet.Types.Int32, -1},
		{"uint32", NewIntLogicalType(32, false), parquet.Types.Int32, -1},
		{"uint64", NewIntLogicalType(64, false), parquet.Types.Int64, -1},
		{"int8", NewIntLogicalType(8, true), parquet.Types.Int32, -1},
		{"int16", NewIntLogicalType(16, true), parquet.Types.Int32, -1},
		{"int32", NewIntLogicalType(32, true), parquet.Types.Int32, -1},
		{"int64", NewIntLogicalType(64, true), parquet.Types.Int64, -1},
		{"null", NullLogicalType{}, parquet.Types.Boolean, -1},
		{"json", JSONLogicalType{}, parquet.Types.ByteArray, -1},
		{"bson", BSONLogicalType{}, parquet.Types.ByteArray, -1},
		{"uuid", UUIDLogicalType{}, parquet.Types.FixedLenByteArray, 16},
		{"none", NoLogicalType{}, parquet.Types.Boolean, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NewPrimitiveNodeLogical("something", parquet.Repetitions.Required, tt.logical, tt.physical, tt.len, -1)
			elem := n.toThrift()
			recover := PrimitiveNodeFromThrift(elem, -1)
			assert.True(t, n.Equals(recover))
		})
	}

	n := NewGroupNodeLogical("map", parquet.Repetitions.Required, []Node{}, MapLogicalType{}, -1)
	elem := n.toThrift()
	recover := GroupNodeFromThrift(elem, []Node{}, -1)
	assert.True(t, recover.Equals(n))

	n = NewGroupNodeLogical("list", parquet.Repetitions.Required, []Node{}, ListLogicalType{}, -1)
	elem = n.toThrift()
	recover = GroupNodeFromThrift(elem, []Node{}, -1)
	assert.True(t, recover.Equals(n))
}

func TestSchemaElementConstruction(t *testing.T) {
	suite.Run(t, new(SchemaElementConstructionSuite))
}
