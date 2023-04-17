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

	"github.com/apache/arrow/go/v12/parquet"
	format "github.com/apache/arrow/go/v12/parquet/internal/gen-go/parquet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func NewPrimitive(name string, repetition format.FieldRepetitionType, typ format.Type, fieldID int32) *format.SchemaElement {
	ret := &format.SchemaElement{
		Name:           name,
		RepetitionType: format.FieldRepetitionTypePtr(repetition),
		Type:           format.TypePtr(typ),
	}
	if fieldID >= 0 {
		ret.FieldID = &fieldID
	}
	return ret
}

func NewGroup(name string, repetition format.FieldRepetitionType, numChildren, fieldID int32) *format.SchemaElement {
	ret := &format.SchemaElement{
		Name:           name,
		RepetitionType: format.FieldRepetitionTypePtr(repetition),
		NumChildren:    &numChildren,
	}
	if fieldID >= 0 {
		ret.FieldID = &fieldID
	}
	return ret
}

type SchemaFlattenSuite struct {
	suite.Suite

	name string
}

func (s *SchemaFlattenSuite) SetupSuite() {
	s.name = "parquet_schema"
}

func (s *SchemaFlattenSuite) TestDecimalMetadata() {
	group := MustGroup(NewGroupNodeConverted("group" /* name */, parquet.Repetitions.Repeated, FieldList{
		MustPrimitive(NewPrimitiveNodeConverted("decimal" /* name */, parquet.Repetitions.Required, parquet.Types.Int64,
			ConvertedTypes.Decimal, 0 /* type len */, 8 /* precision */, 4 /* scale */, -1 /* fieldID */)),
	}, ConvertedTypes.List, -1 /* fieldID */))
	elements := ToThrift(group)

	s.Len(elements, 2)
	s.Equal("decimal", elements[1].GetName())
	s.True(elements[1].IsSetPrecision())
	s.True(elements[1].IsSetScale())

	group = MustGroup(NewGroupNodeLogical("group" /* name */, parquet.Repetitions.Repeated, FieldList{
		MustPrimitive(NewPrimitiveNodeLogical("decimal" /* name */, parquet.Repetitions.Required, NewDecimalLogicalType(10 /* precision */, 5 /* scale */),
			parquet.Types.Int64, 0 /* type len */, -1 /* fieldID */)),
	}, NewListLogicalType(), -1 /* fieldID */))
	elements = ToThrift(group)
	s.Equal("decimal", elements[1].Name)
	s.True(elements[1].IsSetPrecision())
	s.True(elements[1].IsSetScale())

	group = MustGroup(NewGroupNodeConverted("group" /* name */, parquet.Repetitions.Repeated, FieldList{
		NewInt64Node("int64" /* name */, parquet.Repetitions.Required, -1 /* fieldID */)}, ConvertedTypes.List, -1 /* fieldID */))
	elements = ToThrift(group)
	s.Equal("int64", elements[1].Name)
	s.False(elements[0].IsSetPrecision())
	s.False(elements[1].IsSetPrecision())
	s.False(elements[0].IsSetScale())
	s.False(elements[1].IsSetScale())
}

func (s *SchemaFlattenSuite) TestNestedExample() {
	elements := make([]*format.SchemaElement, 0)
	elements = append(elements,
		NewGroup(s.name, format.FieldRepetitionType_REPEATED, 2 /* numChildren */, 0 /* fieldID */),
		NewPrimitive("a" /* name */, format.FieldRepetitionType_REQUIRED, format.Type_INT32, 1 /* fieldID */),
		NewGroup("bag" /* name */, format.FieldRepetitionType_OPTIONAL, 1 /* numChildren */, 2 /* fieldID */))

	elt := NewGroup("b" /* name */, format.FieldRepetitionType_REPEATED, 1 /* numChildren */, 3 /* fieldID */)
	elt.ConvertedType = format.ConvertedTypePtr(format.ConvertedType_LIST)
	elt.LogicalType = &format.LogicalType{LIST: format.NewListType()}
	elements = append(elements, elt, NewPrimitive("item" /* name */, format.FieldRepetitionType_OPTIONAL, format.Type_INT64, 4 /* fieldID */))

	fields := FieldList{NewInt32Node("a" /* name */, parquet.Repetitions.Required, 1 /* fieldID */)}
	list := MustGroup(NewGroupNodeConverted("b" /* name */, parquet.Repetitions.Repeated, FieldList{
		NewInt64Node("item" /* name */, parquet.Repetitions.Optional, 4 /* fieldID */)}, ConvertedTypes.List, 3 /* fieldID */))
	fields = append(fields, MustGroup(NewGroupNode("bag" /* name */, parquet.Repetitions.Optional, FieldList{list}, 2 /* fieldID */)))

	sc := MustGroup(NewGroupNode(s.name, parquet.Repetitions.Repeated, fields, 0 /* fieldID */))

	flattened := ToThrift(sc)
	s.Len(flattened, len(elements))
	for idx, elem := range flattened {
		s.Equal(elements[idx], elem)
	}
}

func TestSchemaFlatten(t *testing.T) {
	suite.Run(t, new(SchemaFlattenSuite))
}

func TestInvalidConvertedTypeInDeserialize(t *testing.T) {
	n := MustPrimitive(NewPrimitiveNodeLogical("string" /* name */, parquet.Repetitions.Required, StringLogicalType{},
		parquet.Types.ByteArray, -1 /* type len */, -1 /* fieldID */))
	assert.True(t, n.LogicalType().Equals(StringLogicalType{}))
	assert.True(t, n.LogicalType().IsValid())
	assert.True(t, n.LogicalType().IsSerialized())
	intermediary := n.toThrift()
	// corrupt it
	intermediary.LogicalType.STRING = nil
	assert.Panics(t, func() {
		PrimitiveNodeFromThrift(intermediary)
	})
}

func TestInvalidTimeUnitInTimeLogical(t *testing.T) {
	n := MustPrimitive(NewPrimitiveNodeLogical("time" /* name */, parquet.Repetitions.Required,
		NewTimeLogicalType(true /* adjustedToUTC */, TimeUnitNanos), parquet.Types.Int64, -1 /* type len */, -1 /* fieldID */))
	intermediary := n.toThrift()
	// corrupt it
	intermediary.LogicalType.TIME.Unit.NANOS = nil
	assert.Panics(t, func() {
		PrimitiveNodeFromThrift(intermediary)
	})
}

func TestInvalidTimeUnitInTimestampLogical(t *testing.T) {
	n := MustPrimitive(NewPrimitiveNodeLogical("time" /* name */, parquet.Repetitions.Required,
		NewTimestampLogicalType(true /* adjustedToUTC */, TimeUnitNanos), parquet.Types.Int64, -1 /* type len */, -1 /* fieldID */))
	intermediary := n.toThrift()
	// corrupt it
	intermediary.LogicalType.TIMESTAMP.Unit.NANOS = nil
	assert.Panics(t, func() {
		PrimitiveNodeFromThrift(intermediary)
	})
}
