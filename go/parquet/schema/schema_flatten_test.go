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
	group := NewGroupNodeConverted("group", parquet.Repetitions.Repeated, FieldList{
		NewPrimitiveNodeConverted("decimal", parquet.Repetitions.Required, parquet.Types.Int64, ConvertedTypes.Decimal, 0, 8, 4, -1),
	}, ConvertedTypes.List, -1)
	elements := ToThrift(group)

	s.Len(elements, 2)
	s.Equal("decimal", elements[1].GetName())
	s.True(elements[1].IsSetPrecision())
	s.True(elements[1].IsSetScale())

	group = NewGroupNodeLogical("group", parquet.Repetitions.Repeated, FieldList{
		NewPrimitiveNodeLogical("decimal", parquet.Repetitions.Required, NewDecimalLogicalType(10, 5), parquet.Types.Int64, 0, -1),
	}, NewListLogicalType(), -1)
	elements = ToThrift(group)
	s.Equal("decimal", elements[1].Name)
	s.True(elements[1].IsSetPrecision())
	s.True(elements[1].IsSetScale())

	group = NewGroupNodeConverted("group", parquet.Repetitions.Repeated, FieldList{NewInt64Node("int64", parquet.Repetitions.Required, -1)}, ConvertedTypes.List, -1)
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
		NewGroup(s.name, format.FieldRepetitionType_REPEATED, 2, 0),
		NewPrimitive("a", format.FieldRepetitionType_REQUIRED, format.Type_INT32, 1),
		NewGroup("bag", format.FieldRepetitionType_OPTIONAL, 1, 2))

	elt := NewGroup("b", format.FieldRepetitionType_REPEATED, 1, 3)
	elt.ConvertedType = format.ConvertedTypePtr(format.ConvertedType_LIST)
	elt.LogicalType = &format.LogicalType{LIST: format.NewListType()}
	elements = append(elements, elt, NewPrimitive("item", format.FieldRepetitionType_OPTIONAL, format.Type_INT64, 4))

	fields := FieldList{NewInt32Node("a", parquet.Repetitions.Required, 1)}
	list := NewGroupNodeConverted("b", parquet.Repetitions.Repeated, FieldList{NewInt64Node("item", parquet.Repetitions.Optional, 4)}, ConvertedTypes.List, 3)
	fields = append(fields, NewGroupNode("bag", parquet.Repetitions.Optional, FieldList{list}, 2))

	sc := NewGroupNode(s.name, parquet.Repetitions.Repeated, fields, 0)

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
	n := NewPrimitiveNodeLogical("string", parquet.Repetitions.Required, StringLogicalType{}, parquet.Types.ByteArray, -1, -1)
	assert.True(t, n.LogicalType().Equals(StringLogicalType{}))
	assert.True(t, n.LogicalType().IsValid())
	assert.True(t, n.LogicalType().IsSerialized())
	intermediary := n.toThrift()
	// corrupt it
	intermediary.LogicalType.STRING = nil
	assert.Panics(t, func() {
		PrimitiveNodeFromThrift(intermediary, 1)
	})
}

func TestInvalidTimeUnitInTimeLogical(t *testing.T) {
	n := NewPrimitiveNodeLogical("time", parquet.Repetitions.Required, NewTimeLogicalType(true, TimeUnitNanos), parquet.Types.Int64, -1, -1)
	intermediary := n.toThrift()
	// corrupt it
	intermediary.LogicalType.TIME.Unit.NANOS = nil
	assert.Panics(t, func() {
		PrimitiveNodeFromThrift(intermediary, 1)
	})
}

func TestInvalidTimeUnitInTimestampLogical(t *testing.T) {
	n := NewPrimitiveNodeLogical("time", parquet.Repetitions.Required, NewTimestampLogicalType(true, TimeUnitNanos), parquet.Types.Int64, -1, -1)
	intermediary := n.toThrift()
	// corrupt it
	intermediary.LogicalType.TIMESTAMP.Unit.NANOS = nil
	assert.Panics(t, func() {
		PrimitiveNodeFromThrift(intermediary, 1)
	})
}
