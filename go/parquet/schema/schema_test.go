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
	"os"
	"testing"

	"github.com/apache/arrow/go/parquet"
	format "github.com/apache/arrow/go/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow/go/parquet/schema"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestColumnPath(t *testing.T) {
	p := parquet.ColumnPath([]string{"toplevel", "leaf"})
	assert.Equal(t, "toplevel.leaf", p.String())

	p2 := parquet.ColumnPathFromString("toplevel.leaf")
	assert.Equal(t, "toplevel.leaf", p2.String())

	extend := p2.Extend("anotherlevel")
	assert.Equal(t, "toplevel.leaf.anotherlevel", extend.String())
}

func NewPrimitive(name string, repetition format.FieldRepetitionType, typ format.Type, fieldId int32) *format.SchemaElement {
	ret := &format.SchemaElement{
		Name:           name,
		RepetitionType: format.FieldRepetitionTypePtr(repetition),
		Type:           format.TypePtr(typ),
	}
	if fieldId >= 0 {
		ret.FieldID = &fieldId
	}
	return ret
}

func NewGroup(name string, repetition format.FieldRepetitionType, numChildren, fieldId int32) *format.SchemaElement {
	ret := &format.SchemaElement{
		Name:           name,
		RepetitionType: format.FieldRepetitionTypePtr(repetition),
		NumChildren:    &numChildren,
	}
	if fieldId >= 0 {
		ret.FieldID = &fieldId
	}
	return ret
}

func TestSchemaNodes(t *testing.T) {
	suite.Run(t, new(PrimitiveNodeTestSuite))
	suite.Run(t, new(GroupNodeTestSuite))
	suite.Run(t, new(SchemaConverterSuite))
}

type PrimitiveNodeTestSuite struct {
	suite.Suite

	name    string
	fieldID int32
	node    schema.Node
}

func (p *PrimitiveNodeTestSuite) SetupTest() {
	p.name = "name"
	p.fieldID = 5
}

func (p *PrimitiveNodeTestSuite) convert(elt *format.SchemaElement) {
	p.node = schema.PrimitiveNodeFromThrift(elt, p.fieldID)
	p.IsType(&schema.PrimitiveNode{}, p.node)
}

func (p *PrimitiveNodeTestSuite) TestAttrs() {
	node1 := schema.NewInt32Node("foo", parquet.Repetitions.Repeated, -1)
	node2 := schema.NewPrimitiveNodeConverted("bar", parquet.Repetitions.Optional, parquet.Types.ByteArray, schema.ConvertedTypes.UTF8, 0, 0, 0, -1)

	p.Equal("foo", node1.Name())
	p.Equal(schema.Primitive, node1.Type())
	p.Equal(schema.Primitive, node2.Type())

	p.Equal(parquet.Repetitions.Repeated, node1.RepetitionType())
	p.Equal(parquet.Repetitions.Optional, node2.RepetitionType())

	p.Equal(parquet.Types.Int32, node1.PhysicalType())
	p.Equal(parquet.Types.ByteArray, node2.PhysicalType())

	p.Equal(schema.ConvertedTypes.None, node1.ConvertedType())
	p.Equal(schema.ConvertedTypes.UTF8, node2.ConvertedType())
}

func (p *PrimitiveNodeTestSuite) TestFromParquet() {
	p.Run("Optional Int32", func() {
		elt := NewPrimitive(p.name, format.FieldRepetitionType_OPTIONAL, format.Type_INT32, p.fieldID)
		p.convert(elt)

		p.Equal(p.name, p.node.Name())
		p.Equal(p.fieldID, p.node.FieldID())
		p.Equal(parquet.Repetitions.Optional, p.node.RepetitionType())
		p.Equal(parquet.Types.Int32, p.node.(*schema.PrimitiveNode).PhysicalType())
		p.Equal(schema.ConvertedTypes.None, p.node.ConvertedType())
	})

	p.Run("LogicalType", func() {
		elt := NewPrimitive(p.name, format.FieldRepetitionType_REQUIRED, format.Type_BYTE_ARRAY, p.fieldID)
		elt.ConvertedType = format.ConvertedTypePtr(format.ConvertedType_UTF8)
		p.convert(elt)

		p.Equal(parquet.Repetitions.Required, p.node.RepetitionType())
		p.Equal(parquet.Types.ByteArray, p.node.(*schema.PrimitiveNode).PhysicalType())
		p.Equal(schema.ConvertedTypes.UTF8, p.node.ConvertedType())
	})

	p.Run("FixedLenByteArray", func() {
		elt := NewPrimitive(p.name, format.FieldRepetitionType_OPTIONAL, format.Type_FIXED_LEN_BYTE_ARRAY, p.fieldID)
		elt.TypeLength = thrift.Int32Ptr(16)
		p.convert(elt)

		p.Equal(p.name, p.node.Name())
		p.Equal(p.fieldID, p.node.FieldID())
		p.Equal(parquet.Repetitions.Optional, p.node.RepetitionType())
		p.Equal(parquet.Types.FixedLenByteArray, p.node.(*schema.PrimitiveNode).PhysicalType())
		p.Equal(16, p.node.(*schema.PrimitiveNode).TypeLength())
	})

	p.Run("convertedtype::decimal", func() {
		elt := NewPrimitive(p.name, format.FieldRepetitionType_OPTIONAL, format.Type_FIXED_LEN_BYTE_ARRAY, p.fieldID)
		elt.ConvertedType = format.ConvertedTypePtr(format.ConvertedType_DECIMAL)
		elt.TypeLength = thrift.Int32Ptr(6)
		elt.Scale = thrift.Int32Ptr(2)
		elt.Precision = thrift.Int32Ptr(12)

		p.convert(elt)
		p.Equal(parquet.Types.FixedLenByteArray, p.node.(*schema.PrimitiveNode).PhysicalType())
		p.Equal(schema.ConvertedTypes.Decimal, p.node.ConvertedType())
		p.Equal(6, p.node.(*schema.PrimitiveNode).TypeLength())
		p.EqualValues(2, p.node.(*schema.PrimitiveNode).DecimalMetadata().Scale)
		p.EqualValues(12, p.node.(*schema.PrimitiveNode).DecimalMetadata().Precision)
	})
}

func (p *PrimitiveNodeTestSuite) TestEquals() {
	node1 := schema.NewInt32Node("foo", parquet.Repetitions.Required, -1)
	node2 := schema.NewInt64Node("foo", parquet.Repetitions.Required, -1)
	node3 := schema.NewInt32Node("bar", parquet.Repetitions.Required, -1)
	node4 := schema.NewInt32Node("foo", parquet.Repetitions.Optional, -1)
	node5 := schema.NewInt32Node("foo", parquet.Repetitions.Required, -1)

	p.True(node1.Equals(node1))
	p.False(node1.Equals(node2))
	p.False(node1.Equals(node3))
	p.False(node1.Equals(node4))
	p.True(node1.Equals(node5))

	flba1 := schema.NewPrimitiveNodeConverted("foo", parquet.Repetitions.Required, parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Decimal, 12, 4, 2, -1)
	flba2 := schema.NewPrimitiveNodeConverted("foo", parquet.Repetitions.Required, parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Decimal, 1, 4, 2, -1)
	flba2.SetTypeLength(12)

	flba3 := schema.NewPrimitiveNodeConverted("foo", parquet.Repetitions.Required, parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Decimal, 1, 4, 2, -1)
	flba3.SetTypeLength(16)

	flba4 := schema.NewPrimitiveNodeConverted("foo", parquet.Repetitions.Required, parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Decimal, 12, 4, 0, -1)
	flba5 := schema.NewPrimitiveNodeConverted("foo", parquet.Repetitions.Required, parquet.Types.FixedLenByteArray, schema.ConvertedTypes.None, 12, 4, 0, -1)

	p.True(flba1.Equals(flba2))
	p.False(flba1.Equals(flba3))
	p.False(flba1.Equals(flba4))
	p.False(flba1.Equals(flba5))
}

func (p *PrimitiveNodeTestSuite) TestPhysicalLogicalMapping() {
	p.NotPanics(func() {
		schema.NewPrimitiveNodeConverted("foo", parquet.Repetitions.Required, parquet.Types.Int32, schema.ConvertedTypes.Int32, 0, 0, 0, -1)
	})
	p.NotPanics(func() {
		schema.NewPrimitiveNodeConverted("foo", parquet.Repetitions.Required, parquet.Types.ByteArray, schema.ConvertedTypes.JSON, 0, 0, 0, -1)
	})
	p.Panics(func() {
		schema.NewPrimitiveNodeConverted("foo", parquet.Repetitions.Required, parquet.Types.Int32, schema.ConvertedTypes.JSON, 0, 0, 0, -1)
	})
	p.NotPanics(func() {
		schema.NewPrimitiveNodeConverted("foo", parquet.Repetitions.Required, parquet.Types.Int64, schema.ConvertedTypes.TimestampMillis, 0, 0, 0, -1)
	})
	p.Panics(func() {
		schema.NewPrimitiveNodeConverted("foo", parquet.Repetitions.Required, parquet.Types.Int32, schema.ConvertedTypes.Int64, 0, 0, 0, -1)
	})
	p.Panics(func() {
		schema.NewPrimitiveNodeConverted("foo", parquet.Repetitions.Required, parquet.Types.ByteArray, schema.ConvertedTypes.Int8, 0, 0, 0, -1)
	})
	p.Panics(func() {
		schema.NewPrimitiveNodeConverted("foo", parquet.Repetitions.Required, parquet.Types.ByteArray, schema.ConvertedTypes.Interval, 0, 0, 0, -1)
	})
	p.Panics(func() {
		schema.NewPrimitiveNodeConverted("foo", parquet.Repetitions.Required, parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Enum, 0, 0, 0, -1)
	})
	p.NotPanics(func() {
		schema.NewPrimitiveNodeConverted("foo", parquet.Repetitions.Required, parquet.Types.ByteArray, schema.ConvertedTypes.Enum, 0, 0, 0, -1)
	})
	p.Panics(func() {
		schema.NewPrimitiveNodeConverted("foo", parquet.Repetitions.Required, parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Decimal, 0, 2, 4, -1)
	})
	p.Panics(func() {
		schema.NewPrimitiveNodeConverted("foo", parquet.Repetitions.Required, parquet.Types.Float, schema.ConvertedTypes.Decimal, 0, 2, 4, -1)
	})
	p.Panics(func() {
		schema.NewPrimitiveNodeConverted("foo", parquet.Repetitions.Required, parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Decimal, 0, 4, 0, -1)
	})
	p.Panics(func() {
		schema.NewPrimitiveNodeConverted("foo", parquet.Repetitions.Required, parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Decimal, 10, 4, -1, -1)
	})
	p.Panics(func() {
		schema.NewPrimitiveNodeConverted("foo", parquet.Repetitions.Required, parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Decimal, 10, 2, 4, -1)
	})
	p.NotPanics(func() {
		schema.NewPrimitiveNodeConverted("foo", parquet.Repetitions.Required, parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Decimal, 10, 6, 4, -1)
	})
	p.NotPanics(func() {
		schema.NewPrimitiveNodeConverted("foo", parquet.Repetitions.Required, parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Interval, 12, 0, 0, -1)
	})
	p.Panics(func() {
		schema.NewPrimitiveNodeConverted("foo", parquet.Repetitions.Required, parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Interval, 10, 0, 0, -1)
	})
}

type GroupNodeTestSuite struct {
	suite.Suite
}

func (g *GroupNodeTestSuite) fields1() []schema.Node {
	return schema.FieldList{
		schema.NewInt32Node("one", parquet.Repetitions.Required, -1),
		schema.NewInt64Node("two", parquet.Repetitions.Optional, -1),
		schema.NewFloat64Node("three", parquet.Repetitions.Optional, -1),
	}
}

func (g *GroupNodeTestSuite) fields2() []schema.Node {
	return schema.FieldList{
		schema.NewInt32Node("duplicate", parquet.Repetitions.Required, -1),
		schema.NewInt64Node("unique", parquet.Repetitions.Optional, -1),
		schema.NewFloat64Node("duplicate", parquet.Repetitions.Optional, -1),
	}
}

func (g *GroupNodeTestSuite) TestAttrs() {
	fields := g.fields1()

	node1 := schema.NewGroupNode("foo", parquet.Repetitions.Repeated, fields, -1)
	node2 := schema.NewGroupNodeConverted("bar", parquet.Repetitions.Optional, fields, schema.ConvertedTypes.List, -1)

	g.Equal("foo", node1.Name())
	g.Equal(schema.Group, node1.Type())
	g.Equal(len(fields), node1.NumFields())
	g.Equal(parquet.Repetitions.Repeated, node1.RepetitionType())
	g.Equal(parquet.Repetitions.Optional, node2.RepetitionType())

	g.Equal(schema.ConvertedTypes.None, node1.ConvertedType())
	g.Equal(schema.ConvertedTypes.List, node2.ConvertedType())
}

func (g *GroupNodeTestSuite) TestEquals() {
	f1 := g.fields1()
	f2 := g.fields1()

	group1 := schema.NewGroupNode("group", parquet.Repetitions.Repeated, f1, -1)
	group2 := schema.NewGroupNode("group", parquet.Repetitions.Repeated, f2, -1)
	group3 := schema.NewGroupNode("group2", parquet.Repetitions.Repeated, f2, -1)

	f2 = append(f2, schema.NewFloat32Node("four", parquet.Repetitions.Optional, -1))
	group4 := schema.NewGroupNode("group", parquet.Repetitions.Repeated, f2, -1)
	group5 := schema.NewGroupNode("group", parquet.Repetitions.Repeated, g.fields1(), -1)

	g.True(group1.Equals(group1))
	g.True(group1.Equals(group2))
	g.False(group1.Equals(group3))
	g.False(group1.Equals(group4))
	g.False(group5.Equals(group4))
}

func (g *GroupNodeTestSuite) TestFieldIndex() {
	fields := g.fields1()
	group := schema.NewGroupNode("group", parquet.Repetitions.Required, fields, -1)
	for idx, field := range fields {
		f := group.Field(idx)
		g.Same(field, f)
		g.Equal(idx, group.FieldIndexByField(f))
		g.Equal(idx, group.FieldIndexByName(field.Name()))
	}

	// Non field nodes
	nonFieldAlien := schema.NewInt32Node("alien", parquet.Repetitions.Required, -1)
	nonFieldFamiliar := schema.NewInt32Node("one", parquet.Repetitions.Repeated, -1)
	g.Less(group.FieldIndexByField(nonFieldAlien), 0)
	g.Less(group.FieldIndexByField(nonFieldFamiliar), 0)
}

func (g *GroupNodeTestSuite) TestFieldIndexDuplicateName() {
	fields := g.fields2()
	group := schema.NewGroupNode("group", parquet.Repetitions.Required, fields, -1)
	for idx, field := range fields {
		f := group.Field(idx)
		g.Same(f, field)
		g.Equal(idx, group.FieldIndexByField(f))
	}
}

type SchemaConverterSuite struct {
	suite.Suite

	name string
	node schema.Node
}

func (s *SchemaConverterSuite) SetupSuite() {
	s.name = "parquet_schema"
}

func (s *SchemaConverterSuite) convert(elems []*format.SchemaElement) {
	s.node = schema.FromParquet(elems)
	s.Equal(schema.Group, s.node.Type())
}

func (s *SchemaConverterSuite) checkParentConsistency(groupRoot *schema.GroupNode) bool {
	// each node should have the group as parent
	for i := 0; i < groupRoot.NumFields(); i++ {
		field := groupRoot.Field(i)
		if field.Parent() != groupRoot {
			return false
		}
		if field.Type() == schema.Group {
			if !s.checkParentConsistency(field.(*schema.GroupNode)) {
				return false
			}
		}
	}
	return true
}

func (s *SchemaConverterSuite) TestNestedExample() {
	elements := make([]*format.SchemaElement, 0)
	elements = append(elements,
		NewGroup(s.name, format.FieldRepetitionType_REPEATED, 2, 0),
		NewPrimitive("a", format.FieldRepetitionType_REQUIRED, format.Type_INT32, 1),
		NewGroup("bag", format.FieldRepetitionType_OPTIONAL, 1, 2))
	elt := NewGroup("b", format.FieldRepetitionType_REPEATED, 1, 3)
	elt.ConvertedType = format.ConvertedTypePtr(format.ConvertedType_LIST)
	elements = append(elements, elt, NewPrimitive("item", format.FieldRepetitionType_OPTIONAL, format.Type_INT64, 4))

	s.convert(elements)

	// construct the expected schema
	fields := make([]schema.Node, 0)
	fields = append(fields, schema.NewInt32Node("a", parquet.Repetitions.Required, 1))

	// 3-level list encoding
	item := schema.NewInt64Node("item", parquet.Repetitions.Optional, 4)
	list := schema.NewGroupNodeConverted("b", parquet.Repetitions.Repeated, schema.FieldList{item}, schema.ConvertedTypes.List, 3)
	bag := schema.NewGroupNode("bag", parquet.Repetitions.Optional, schema.FieldList{list}, 2)
	fields = append(fields, bag)

	sc := schema.NewGroupNode(s.name, parquet.Repetitions.Repeated, fields, 0)
	s.True(sc.Equals(s.node))
	s.Nil(s.node.Parent())
	s.True(s.checkParentConsistency(s.node.(*schema.GroupNode)))
}

func (s *SchemaConverterSuite) TestZeroColumns() {
	elements := []*format.SchemaElement{NewGroup("schema", format.FieldRepetitionType_REPEATED, 0, 0)}
	s.NotPanics(func() { s.convert(elements) })
}

func (s *SchemaConverterSuite) TestInvalidRoot() {
	// According to the Parquet spec, the first element in the list<SchemaElement>
	// is a group whose children (and their descendants) contain all of the rest of
	// the flattened schema elments. If the first element is not a group, it is malformed
	elements := []*format.SchemaElement{NewPrimitive("not-a-group", format.FieldRepetitionType_REQUIRED, format.Type_INT32, 0), format.NewSchemaElement()}
	s.Panics(func() { s.convert(elements) })

	// While the parquet spec indicates that the root group should have REPEATED
	// repetition type, some implementations may return REQUIRED or OPTIONAL
	// groups as the first element. These tests check that this is okay as a
	// practicality matter
	elements = []*format.SchemaElement{
		NewGroup("not-repeated", format.FieldRepetitionType_REQUIRED, 1, 0),
		NewPrimitive("a", format.FieldRepetitionType_REQUIRED, format.Type_INT32, 1)}
	s.NotPanics(func() { s.convert(elements) })

	elements[0] = NewGroup("not-repeated", format.FieldRepetitionType_OPTIONAL, 1, 0)
	s.NotPanics(func() { s.convert(elements) })
}

func (s *SchemaConverterSuite) TestNotEnoughChildren() {
	s.Panics(func() {
		s.convert([]*format.SchemaElement{NewGroup(s.name, format.FieldRepetitionType_REPEATED, 2, 0)})
	})
}

func TestColumnDesc(t *testing.T) {
	n := schema.NewPrimitiveNodeConverted("name", parquet.Repetitions.Optional, parquet.Types.ByteArray, schema.ConvertedTypes.UTF8, 0, 0, 0, -1)
	descr := schema.NewColumn(n, 4, 1)

	assert.Equal(t, "name", descr.Name())
	assert.EqualValues(t, 4, descr.MaxDefinitionLevel())
	assert.EqualValues(t, 1, descr.MaxRepetitionLevel())
	assert.Equal(t, parquet.Types.ByteArray, descr.PhysicalType())
	assert.Equal(t, -1, descr.TypeLength())

	expectedDesc := `column descriptor = {
  name: name,
  path: ,
  physical_type: BYTE_ARRAY,
  converted_type: UTF8,
  logical_type: String,
  max_definition_level: 4,
  max_repetition_level: 1,
}`
	assert.Equal(t, expectedDesc, descr.String())

	n = schema.NewPrimitiveNodeConverted("name", parquet.Repetitions.Optional, parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Decimal, 12, 10, 4, -1)
	descr2 := schema.NewColumn(n, 4, 1)

	assert.Equal(t, parquet.Types.FixedLenByteArray, descr2.PhysicalType())
	assert.Equal(t, 12, descr2.TypeLength())

	expectedDesc = `column descriptor = {
  name: name,
  path: ,
  physical_type: FIXED_LEN_BYTE_ARRAY,
  converted_type: DECIMAL,
  logical_type: Decimal(precision=10, scale=4),
  max_definition_level: 4,
  max_repetition_level: 1,
  length: 12,
  precision: 10,
  scale: 4,
}`
	assert.Equal(t, expectedDesc, descr2.String())
}

func TestSchemaDescriptor(t *testing.T) {
	t.Run("InitNonGroup", func(t *testing.T) {
		assert.Panics(t, func() {
			n := schema.NewInt32Node("field", parquet.Repetitions.Optional, -1)
			schema.NewSchema(n)
		})
	})

	t.Run("Equals", func(t *testing.T) {
		inta := schema.NewInt32Node("a", parquet.Repetitions.Required, -1)
		intb := schema.NewInt64Node("b", parquet.Repetitions.Optional, -1)
		intb2 := schema.NewInt64Node("b2", parquet.Repetitions.Optional, -1)
		intc := schema.NewByteArrayNode("c", parquet.Repetitions.Repeated, -1)

		item1 := schema.NewInt64Node("item1", parquet.Repetitions.Required, -1)
		item2 := schema.NewBooleanNode("item2", parquet.Repetitions.Optional, -1)
		item3 := schema.NewInt32Node("item3", parquet.Repetitions.Repeated, -1)
		list := schema.NewGroupNodeConverted("records", parquet.Repetitions.Repeated, schema.FieldList{item1, item2, item3}, schema.ConvertedTypes.List, -1)

		bag := schema.NewGroupNode("bag", parquet.Repetitions.Optional, schema.FieldList{list}, -1)
		bag2 := schema.NewGroupNode("bag", parquet.Repetitions.Required, schema.FieldList{list}, -1)

		descr1 := schema.NewSchema(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{inta, intb, intc, bag}, -1))
		assert.True(t, descr1.Equals(descr1))

		descr2 := schema.NewSchema(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{inta, intb, intc, bag2}, -1))
		assert.False(t, descr1.Equals(descr2))

		descr3 := schema.NewSchema(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{inta, intb2, intc, bag}, -1))
		assert.False(t, descr1.Equals(descr3))

		descr4 := schema.NewSchema(schema.NewGroupNode("SCHEMA", parquet.Repetitions.Repeated, schema.FieldList{inta, intb, intc, bag}, -1))
		assert.True(t, descr1.Equals(descr4))

		descr5 := schema.NewSchema(schema.NewGroupNode("schema", parquet.Repetitions.Repeated, schema.FieldList{inta, intb, intc, bag, intb2}, -1))
		assert.False(t, descr1.Equals(descr5))

		col1 := schema.NewColumn(inta, 5, 1)
		col2 := schema.NewColumn(inta, 6, 1)
		col3 := schema.NewColumn(inta, 5, 2)

		assert.True(t, col1.Equals(col1))
		assert.False(t, col1.Equals(col2))
		assert.False(t, col2.Equals(col3))
	})

	t.Run("BuildTree", func(t *testing.T) {
		inta := schema.NewInt32Node("a", parquet.Repetitions.Required, -1)
		fields := schema.FieldList{inta}
		fields = append(fields,
			schema.NewInt64Node("b", parquet.Repetitions.Optional, -1),
			schema.NewByteArrayNode("c", parquet.Repetitions.Repeated, -1))

		item1 := schema.NewInt64Node("item1", parquet.Repetitions.Required, -1)
		item2 := schema.NewBooleanNode("item2", parquet.Repetitions.Optional, -1)
		item3 := schema.NewInt32Node("item3", parquet.Repetitions.Repeated, -1)
		list := schema.NewGroupNodeConverted("records", parquet.Repetitions.Repeated, schema.FieldList{item1, item2, item3}, schema.ConvertedTypes.List, -1)
		bag := schema.NewGroupNode("bag", parquet.Repetitions.Optional, schema.FieldList{list}, -1)
		fields = append(fields, bag)

		sc := schema.NewGroupNode("schema", parquet.Repetitions.Repeated, fields, -1)
		descr := schema.NewSchema(sc)

		const nleaves = 6
		assert.Equal(t, nleaves, descr.NumColumns())

		//                             mdef mrep
		// required int32 a            0    0
		// optional int64 b            1    0
		// repeated byte_array c       1    1
		// optional group bag          1    0
		//   repeated group records    2    1
		//     required int64 item1    2    1
		//     optional boolean item2  3    1
		//     repeated int32 item3    3    2
		var (
			exMaxDefLevels = [...]int16{0, 1, 1, 2, 3, 3}
			exMaxRepLevels = [...]int16{0, 0, 1, 1, 1, 2}
		)

		for i := 0; i < nleaves; i++ {
			col := descr.Column(i)
			assert.Equal(t, exMaxDefLevels[i], col.MaxDefinitionLevel())
			assert.Equal(t, exMaxRepLevels[i], col.MaxRepetitionLevel())
		}

		assert.Equal(t, "a", descr.Column(0).Path())
		assert.Equal(t, "b", descr.Column(1).Path())
		assert.Equal(t, "c", descr.Column(2).Path())
		assert.Equal(t, "bag.records.item1", descr.Column(3).Path())
		assert.Equal(t, "bag.records.item2", descr.Column(4).Path())
		assert.Equal(t, "bag.records.item3", descr.Column(5).Path())

		for i := 0; i < nleaves; i++ {
			col := descr.Column(i)
			assert.Equal(t, i, descr.ColumnIndexByNode(col.SchemaNode()))
		}

		nonColumnAlien := schema.NewInt32Node("alien", parquet.Repetitions.Required, -1)
		nonColumnFamiliar := schema.NewInt32Node("a", parquet.Repetitions.Repeated, -1)
		assert.Less(t, descr.ColumnIndexByNode(nonColumnAlien), 0)
		assert.Less(t, descr.ColumnIndexByNode(nonColumnFamiliar), 0)

		assert.Same(t, inta, descr.ColumnRoot(0))
		assert.Same(t, bag, descr.ColumnRoot(3))
		assert.Same(t, bag, descr.ColumnRoot(4))
		assert.Same(t, bag, descr.ColumnRoot(5))

		assert.Same(t, sc, descr.Root())
	})

	t.Run("HasRepeatedFields", func(t *testing.T) {
		inta := schema.NewInt32Node("a", parquet.Repetitions.Required, -1)
		fields := schema.FieldList{inta}
		fields = append(fields,
			schema.NewInt64Node("b", parquet.Repetitions.Optional, -1),
			schema.NewByteArrayNode("c", parquet.Repetitions.Repeated, -1))

		sc := schema.NewGroupNode("schema", parquet.Repetitions.Repeated, fields, -1)
		descr := schema.NewSchema(sc)
		assert.True(t, descr.HasRepeatedFields())

		item1 := schema.NewInt64Node("item1", parquet.Repetitions.Required, -1)
		item2 := schema.NewBooleanNode("item2", parquet.Repetitions.Optional, -1)
		item3 := schema.NewInt32Node("item3", parquet.Repetitions.Repeated, -1)
		list := schema.NewGroupNodeConverted("records", parquet.Repetitions.Repeated, schema.FieldList{item1, item2, item3}, schema.ConvertedTypes.List, -1)
		bag := schema.NewGroupNode("bag", parquet.Repetitions.Optional, schema.FieldList{list}, -1)
		fields = append(fields, bag)

		sc = schema.NewGroupNode("schema", parquet.Repetitions.Repeated, fields, -1)
		descr = schema.NewSchema(sc)
		assert.True(t, descr.HasRepeatedFields())

		itemKey := schema.NewInt64Node("key", parquet.Repetitions.Required, -1)
		itemValue := schema.NewBooleanNode("value", parquet.Repetitions.Optional, -1)
		sc = schema.NewGroupNode("schema", parquet.Repetitions.Repeated, append(fields, schema.FieldList{
			schema.NewGroupNode("my_map", parquet.Repetitions.Optional, schema.FieldList{
				schema.NewGroupNodeConverted("map", parquet.Repetitions.Repeated, schema.FieldList{itemKey, itemValue}, schema.ConvertedTypes.Map, -1),
			}, -1),
		}...), -1)
		descr = schema.NewSchema(sc)
		assert.True(t, descr.HasRepeatedFields())
	})
}

func ExamplePrintSchema() {
	fields := schema.FieldList{schema.NewInt32Node("a", parquet.Repetitions.Required, 1)}
	item1 := schema.NewInt64Node("item1", parquet.Repetitions.Optional, 4)
	item2 := schema.NewBooleanNode("item2", parquet.Repetitions.Required, 5)
	list := schema.NewGroupNodeConverted("b", parquet.Repetitions.Repeated, schema.FieldList{item1, item2}, schema.ConvertedTypes.List, 3)
	bag := schema.NewGroupNode("bag", parquet.Repetitions.Optional, schema.FieldList{list}, 2)
	fields = append(fields, bag)

	fields = append(fields,
		schema.NewPrimitiveNodeConverted("c", parquet.Repetitions.Required, parquet.Types.Int32, schema.ConvertedTypes.Decimal, 0, 3, 2, 6),
		schema.NewPrimitiveNodeLogical("d", parquet.Repetitions.Required, schema.NewDecimalLogicalType(10, 5), parquet.Types.Int64, -1, 7))

	sc := schema.NewGroupNode("schema", parquet.Repetitions.Repeated, fields, 0)
	schema.PrintSchema(sc, os.Stdout, 2)

	// Output:
	// repeated group field_id=0 schema {
	//   required int32 field_id=1 a;
	//   optional group field_id=2 bag {
	//     repeated group field_id=3 b (List) {
	//       optional int64 field_id=4 item1;
	//       required boolean field_id=5 item2;
	//     }
	//   }
	//   required int32 field_id=6 c (Decimal(precision=3, scale=2));
	//   required int64 field_id=7 d (Decimal(precision=10, scale=5));
	// }
}

func TestPanicSchemaNodeCreation(t *testing.T) {
	assert.Panics(t, func() {
		schema.NewPrimitiveNodeLogical("map", parquet.Repetitions.Required, schema.MapLogicalType{}, parquet.Types.Int64, -1, -1)
	}, "nested logical type on non-group node")

	assert.Panics(t, func() {
		schema.NewPrimitiveNodeLogical("string", parquet.Repetitions.Required, schema.StringLogicalType{}, parquet.Types.Boolean, -1, -1)
	}, "incompatible primitive type")

	assert.Panics(t, func() {
		schema.NewPrimitiveNodeLogical("interval", parquet.Repetitions.Required, schema.IntervalLogicalType{}, parquet.Types.FixedLenByteArray, 11, -1)
	}, "incompatible primitive length")

	assert.Panics(t, func() {
		schema.NewPrimitiveNodeLogical("decimal", parquet.Repetitions.Required, schema.NewDecimalLogicalType(16, 6), parquet.Types.Int32, -1, -1)
	}, "primitive too small for given precision")

	assert.Panics(t, func() {
		schema.NewPrimitiveNodeLogical("uuid", parquet.Repetitions.Required, schema.UUIDLogicalType{}, parquet.Types.FixedLenByteArray, 64, -1)
	}, "incompatible primitive length")

	assert.Panics(t, func() {
		schema.NewPrimitiveNodeLogical("negative_len", parquet.Repetitions.Required, schema.NoLogicalType{}, parquet.Types.FixedLenByteArray, -16, -1)
	}, "non-positive length for fixed length binary")

	assert.Panics(t, func() {
		schema.NewPrimitiveNodeLogical("zero_len", parquet.Repetitions.Required, schema.NoLogicalType{}, parquet.Types.FixedLenByteArray, 0, -1)
	}, "non-positive length for fixed length binary")

	assert.Panics(t, func() {
		schema.NewGroupNodeLogical("list", parquet.Repetitions.Repeated, schema.FieldList{}, schema.JSONLogicalType{}, -1)
	}, "non-nested logical type on group node")
}

func TestNullLogicalConvertsToNone(t *testing.T) {
	var (
		empty schema.LogicalType
		n     schema.Node
	)
	assert.NotPanics(t, func() {
		n = schema.NewPrimitiveNodeLogical("value", parquet.Repetitions.Required, empty, parquet.Types.Double, -1, -1)
	})
	assert.True(t, n.LogicalType().IsNone())
	assert.Equal(t, schema.ConvertedTypes.None, n.ConvertedType())
	assert.NotPanics(t, func() {
		n = schema.NewGroupNodeLogical("items", parquet.Repetitions.Repeated, schema.FieldList{}, empty, -1)
	})
	assert.True(t, n.LogicalType().IsNone())
	assert.Equal(t, schema.ConvertedTypes.None, n.ConvertedType())
}
