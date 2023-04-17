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

	"github.com/apache/arrow/go/v12/parquet"
	format "github.com/apache/arrow/go/v12/parquet/internal/gen-go/parquet"
	"github.com/apache/arrow/go/v12/parquet/schema"
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
	p.node = schema.MustPrimitive(schema.PrimitiveNodeFromThrift(elt))
	p.IsType(&schema.PrimitiveNode{}, p.node)
}

func (p *PrimitiveNodeTestSuite) TestAttrs() {
	node1 := schema.NewInt32Node("foo" /* name */, parquet.Repetitions.Repeated, -1 /* fieldID */)
	node2 := schema.MustPrimitive(schema.NewPrimitiveNodeConverted("bar" /* name */, parquet.Repetitions.Optional, parquet.Types.ByteArray,
		schema.ConvertedTypes.UTF8, 0 /* type len */, 0 /* precision */, 0 /* scale */, -1 /* fieldID */))

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
	const fieldID = -1
	node1 := schema.NewInt32Node("foo" /* name */, parquet.Repetitions.Required, fieldID)
	node2 := schema.NewInt64Node("foo" /* name */, parquet.Repetitions.Required, fieldID)
	node3 := schema.NewInt32Node("bar" /* name */, parquet.Repetitions.Required, fieldID)
	node4 := schema.NewInt32Node("foo" /* name */, parquet.Repetitions.Optional, fieldID)
	node5 := schema.NewInt32Node("foo" /* name */, parquet.Repetitions.Required, fieldID)

	p.True(node1.Equals(node1))
	p.False(node1.Equals(node2))
	p.False(node1.Equals(node3))
	p.False(node1.Equals(node4))
	p.True(node1.Equals(node5))

	flba1 := schema.MustPrimitive(schema.NewPrimitiveNodeConverted("foo" /* name */, parquet.Repetitions.Required, parquet.Types.FixedLenByteArray,
		schema.ConvertedTypes.Decimal, 12 /* type len */, 4 /* precision */, 2 /* scale */, fieldID))
	flba2 := schema.MustPrimitive(schema.NewPrimitiveNodeConverted("foo" /* name */, parquet.Repetitions.Required, parquet.Types.FixedLenByteArray,
		schema.ConvertedTypes.Decimal, 1 /* type len */, 4 /* precision */, 2 /* scale */, fieldID))
	flba2.SetTypeLength(12)

	flba3 := schema.MustPrimitive(schema.NewPrimitiveNodeConverted("foo" /* name */, parquet.Repetitions.Required, parquet.Types.FixedLenByteArray,
		schema.ConvertedTypes.Decimal, 1 /* type len */, 4 /* precision */, 2 /* scale */, fieldID))
	flba3.SetTypeLength(16)

	flba4 := schema.MustPrimitive(schema.NewPrimitiveNodeConverted("foo" /* name */, parquet.Repetitions.Required, parquet.Types.FixedLenByteArray,
		schema.ConvertedTypes.Decimal, 12 /* type len */, 4 /* precision */, 0 /* scale */, fieldID))
	flba5 := schema.MustPrimitive(schema.NewPrimitiveNodeConverted("foo" /* name */, parquet.Repetitions.Required, parquet.Types.FixedLenByteArray,
		schema.ConvertedTypes.None, 12 /* type len */, 4 /* precision */, 0 /* scale */, fieldID))

	p.True(flba1.Equals(flba2))
	p.False(flba1.Equals(flba3))
	p.False(flba1.Equals(flba4))
	p.False(flba1.Equals(flba5))
}

func (p *PrimitiveNodeTestSuite) TestPhysicalLogicalMapping() {
	tests := []struct {
		typ       parquet.Type
		cnv       schema.ConvertedType
		typLen    int
		precision int
		scale     int
		shouldErr bool
	}{
		{parquet.Types.Int32, schema.ConvertedTypes.Int32, 0 /* type len */, 0 /* precision */, 0 /* scale */, false},
		{parquet.Types.ByteArray, schema.ConvertedTypes.JSON, 0 /* type len */, 0 /* precision */, 0 /* scale */, false},
		{parquet.Types.Int32, schema.ConvertedTypes.JSON, 0 /* type len */, 0 /* precision */, 0 /* scale */, true},
		{parquet.Types.Int64, schema.ConvertedTypes.TimestampMillis, 0 /* type len */, 0 /* precision */, 0 /* scale */, false},
		{parquet.Types.Int32, schema.ConvertedTypes.Int64, 0 /* type len */, 0 /* precision */, 0 /* scale */, true},
		{parquet.Types.ByteArray, schema.ConvertedTypes.Int8, 0 /* type len */, 0 /* precision */, 0 /* scale */, true},
		{parquet.Types.ByteArray, schema.ConvertedTypes.Interval, 0 /* type len */, 0 /* precision */, 0 /* scale */, true},
		{parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Enum, 0 /* type len */, 0 /* precision */, 0 /* scale */, true},
		{parquet.Types.ByteArray, schema.ConvertedTypes.Enum, 0 /* type len */, 0 /* precision */, 0 /* scale */, false},
		{parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Decimal, 0 /* type len */, 2 /* precision */, 4 /* scale */, true},
		{parquet.Types.Float, schema.ConvertedTypes.Decimal, 0 /* type len */, 2 /* precision */, 4 /* scale */, true},
		{parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Decimal, 0 /* type len */, 4 /* precision */, 0 /* scale */, true},
		{parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Decimal, 10 /* type len */, 4 /* precision */, -1 /* scale */, true},
		{parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Decimal, 10 /* type len */, 2 /* precision */, 4 /* scale */, true},
		{parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Decimal, 10 /* type len */, 6 /* precision */, 4 /* scale */, false},
		{parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Interval, 12 /* type len */, 0 /* precision */, 0 /* scale */, false},
		{parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Interval, 10 /* type len */, 0 /* precision */, 0 /* scale */, true},
	}
	for _, tt := range tests {
		p.Run(tt.typ.String(), func() {
			_, err := schema.NewPrimitiveNodeConverted("foo" /* name */, parquet.Repetitions.Required, tt.typ, tt.cnv, tt.typLen, tt.precision, tt.scale, -1 /* fieldID */)
			if tt.shouldErr {
				p.Error(err)
			} else {
				p.NoError(err)
			}
		})
	}
}

type GroupNodeTestSuite struct {
	suite.Suite
}

func (g *GroupNodeTestSuite) fields1() []schema.Node {
	return schema.FieldList{
		schema.NewInt32Node("one" /* name */, parquet.Repetitions.Required, -1 /* fieldID */),
		schema.NewInt64Node("two" /* name */, parquet.Repetitions.Optional, -1 /* fieldID */),
		schema.NewFloat64Node("three" /* name */, parquet.Repetitions.Optional, -1 /* fieldID */),
	}
}

func (g *GroupNodeTestSuite) fields2() []schema.Node {
	return schema.FieldList{
		schema.NewInt32Node("duplicate" /* name */, parquet.Repetitions.Required, -1 /* fieldID */),
		schema.NewInt64Node("unique" /* name */, parquet.Repetitions.Optional, -1 /* fieldID */),
		schema.NewFloat64Node("duplicate" /* name */, parquet.Repetitions.Optional, -1 /* fieldID */),
	}
}

func (g *GroupNodeTestSuite) TestAttrs() {
	fields := g.fields1()

	node1 := schema.MustGroup(schema.NewGroupNode("foo" /* name */, parquet.Repetitions.Repeated, fields, -1 /* fieldID */))
	node2 := schema.MustGroup(schema.NewGroupNodeConverted("bar" /* name */, parquet.Repetitions.Optional, fields, schema.ConvertedTypes.List, -1 /* fieldID */))

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

	group1 := schema.Must(schema.NewGroupNode("group" /* name */, parquet.Repetitions.Repeated, f1, -1 /* fieldID */))
	group2 := schema.Must(schema.NewGroupNode("group" /* name */, parquet.Repetitions.Repeated, f2, -1 /* fieldID */))
	group3 := schema.Must(schema.NewGroupNode("group2" /* name */, parquet.Repetitions.Repeated, f2, -1 /* fieldID */))

	f2 = append(f2, schema.NewFloat32Node("four" /* name */, parquet.Repetitions.Optional, -1 /* fieldID */))
	group4 := schema.Must(schema.NewGroupNode("group" /* name */, parquet.Repetitions.Repeated, f2, -1 /* fieldID */))
	group5 := schema.Must(schema.NewGroupNode("group" /* name */, parquet.Repetitions.Repeated, g.fields1(), -1 /* fieldID */))

	g.True(group1.Equals(group1))
	g.True(group1.Equals(group2))
	g.False(group1.Equals(group3))
	g.False(group1.Equals(group4))
	g.False(group5.Equals(group4))
}

func (g *GroupNodeTestSuite) TestFieldIndex() {
	fields := g.fields1()
	group := schema.MustGroup(schema.NewGroupNode("group" /* name */, parquet.Repetitions.Required, fields, -1 /* fieldID */))
	for idx, field := range fields {
		f := group.Field(idx)
		g.Same(field, f)
		g.Equal(idx, group.FieldIndexByField(f))
		g.Equal(idx, group.FieldIndexByName(field.Name()))
	}

	// Non field nodes
	nonFieldAlien := schema.NewInt32Node("alien" /* name */, parquet.Repetitions.Required, -1 /* fieldID */)
	nonFieldFamiliar := schema.NewInt32Node("one" /* name */, parquet.Repetitions.Repeated, -1 /* fieldID */)
	g.Less(group.FieldIndexByField(nonFieldAlien), 0)
	g.Less(group.FieldIndexByField(nonFieldFamiliar), 0)
}

func (g *GroupNodeTestSuite) TestFieldIndexDuplicateName() {
	fields := g.fields2()
	group := schema.MustGroup(schema.NewGroupNode("group" /* name */, parquet.Repetitions.Required, fields, -1 /* fieldID */))
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
	s.node = schema.Must(schema.FromParquet(elems))
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
		NewGroup(s.name, format.FieldRepetitionType_REPEATED, 2 /* numChildren */, 0 /* fieldID */),
		NewPrimitive("a" /* name */, format.FieldRepetitionType_REQUIRED, format.Type_INT32, 1 /* fieldID */),
		NewGroup("bag" /* name */, format.FieldRepetitionType_OPTIONAL, 1 /* numChildren */, 2 /* fieldID */))
	elt := NewGroup("b" /* name */, format.FieldRepetitionType_REPEATED, 1 /* numChildren */, 3 /* fieldID */)
	elt.ConvertedType = format.ConvertedTypePtr(format.ConvertedType_LIST)
	elements = append(elements, elt, NewPrimitive("item" /* name */, format.FieldRepetitionType_OPTIONAL, format.Type_INT64, 4 /* fieldID */))

	s.convert(elements)

	// construct the expected schema
	fields := make([]schema.Node, 0)
	fields = append(fields, schema.NewInt32Node("a" /* name */, parquet.Repetitions.Required, 1 /* fieldID */))

	// 3-level list encoding
	item := schema.NewInt64Node("item" /* name */, parquet.Repetitions.Optional, 4 /* fieldID */)
	list := schema.MustGroup(schema.NewGroupNodeConverted("b" /* name */, parquet.Repetitions.Repeated, schema.FieldList{item}, schema.ConvertedTypes.List, 3 /* fieldID */))
	bag := schema.MustGroup(schema.NewGroupNode("bag" /* name */, parquet.Repetitions.Optional, schema.FieldList{list}, 2 /* fieldID */))
	fields = append(fields, bag)

	sc := schema.MustGroup(schema.NewGroupNode(s.name, parquet.Repetitions.Repeated, fields, 0 /* fieldID */))
	s.True(sc.Equals(s.node))
	s.Nil(s.node.Parent())
	s.True(s.checkParentConsistency(s.node.(*schema.GroupNode)))
}

func (s *SchemaConverterSuite) TestZeroColumns() {
	elements := []*format.SchemaElement{NewGroup("schema" /* name */, format.FieldRepetitionType_REPEATED, 0 /* numChildren */, 0 /* fieldID */)}
	s.NotPanics(func() { s.convert(elements) })
}

func (s *SchemaConverterSuite) TestInvalidRoot() {
	// According to the Parquet spec, the first element in the list<SchemaElement>
	// is a group whose children (and their descendants) contain all of the rest of
	// the flattened schema elments. If the first element is not a group, it is malformed
	elements := []*format.SchemaElement{NewPrimitive("not-a-group" /* name */, format.FieldRepetitionType_REQUIRED,
		format.Type_INT32, 0 /* fieldID */), format.NewSchemaElement()}
	s.Panics(func() { s.convert(elements) })

	// While the parquet spec indicates that the root group should have REPEATED
	// repetition type, some implementations may return REQUIRED or OPTIONAL
	// groups as the first element. These tests check that this is okay as a
	// practicality matter
	elements = []*format.SchemaElement{
		NewGroup("not-repeated" /* name */, format.FieldRepetitionType_REQUIRED, 1 /* numChildren */, 0 /* fieldID */),
		NewPrimitive("a" /* name */, format.FieldRepetitionType_REQUIRED, format.Type_INT32, 1 /* fieldID */)}
	s.NotPanics(func() { s.convert(elements) })

	elements[0] = NewGroup("not-repeated" /* name */, format.FieldRepetitionType_OPTIONAL, 1 /* numChildren */, 0 /* fieldID */)
	s.NotPanics(func() { s.convert(elements) })
}

func (s *SchemaConverterSuite) TestNotEnoughChildren() {
	s.Panics(func() {
		s.convert([]*format.SchemaElement{NewGroup(s.name, format.FieldRepetitionType_REPEATED, 2 /* numChildren */, 0 /* fieldID */)})
	})
}

func TestColumnDesc(t *testing.T) {
	n := schema.MustPrimitive(schema.NewPrimitiveNodeConverted("name" /* name */, parquet.Repetitions.Optional, parquet.Types.ByteArray,
		schema.ConvertedTypes.UTF8, 0 /* type len */, 0 /* precision */, 0 /* scale */, -1 /* fieldID */))
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

	n = schema.MustPrimitive(schema.NewPrimitiveNodeConverted("name" /* name */, parquet.Repetitions.Optional, parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Decimal, 12 /* type len */, 10 /* precision */, 4 /* scale */, -1 /* fieldID */))
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
	t.Run("Equals", func(t *testing.T) {
		inta := schema.NewInt32Node("a" /* name */, parquet.Repetitions.Required, -1 /* fieldID */)
		intb := schema.NewInt64Node("b" /* name */, parquet.Repetitions.Optional, -1 /* fieldID */)
		intb2 := schema.NewInt64Node("b2" /* name */, parquet.Repetitions.Optional, -1 /* fieldID */)
		intc := schema.NewByteArrayNode("c" /* name */, parquet.Repetitions.Repeated, -1 /* fieldID */)

		item1 := schema.NewInt64Node("item1" /* name */, parquet.Repetitions.Required, -1 /* fieldID */)
		item2 := schema.NewBooleanNode("item2" /* name */, parquet.Repetitions.Optional, -1 /* fieldID */)
		item3 := schema.NewInt32Node("item3" /* name */, parquet.Repetitions.Repeated, -1 /* fieldID */)
		list := schema.MustGroup(schema.NewGroupNodeConverted("records" /* name */, parquet.Repetitions.Repeated, schema.FieldList{item1, item2, item3}, schema.ConvertedTypes.List, -1 /* fieldID */))

		bag := schema.MustGroup(schema.NewGroupNode("bag" /* name */, parquet.Repetitions.Optional, schema.FieldList{list}, -1 /* fieldID */))
		bag2 := schema.MustGroup(schema.NewGroupNode("bag" /* name */, parquet.Repetitions.Required, schema.FieldList{list}, -1 /* fieldID */))

		descr1 := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema" /* name */, parquet.Repetitions.Repeated, schema.FieldList{inta, intb, intc, bag}, -1 /* fieldID */)))
		assert.True(t, descr1.Equals(descr1))

		descr2 := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema" /* name */, parquet.Repetitions.Repeated, schema.FieldList{inta, intb, intc, bag2}, -1 /* fieldID */)))
		assert.False(t, descr1.Equals(descr2))

		descr3 := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema" /* name */, parquet.Repetitions.Repeated, schema.FieldList{inta, intb2, intc, bag}, -1 /* fieldID */)))
		assert.False(t, descr1.Equals(descr3))

		descr4 := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("SCHEMA" /* name */, parquet.Repetitions.Repeated, schema.FieldList{inta, intb, intc, bag}, -1 /* fieldID */)))
		assert.True(t, descr1.Equals(descr4))

		descr5 := schema.NewSchema(schema.MustGroup(schema.NewGroupNode("schema" /* name */, parquet.Repetitions.Repeated, schema.FieldList{inta, intb, intc, bag, intb2}, -1 /* fieldID */)))
		assert.False(t, descr1.Equals(descr5))

		col1 := schema.NewColumn(inta, 5 /* maxDefLvl */, 1 /* maxRepLvl */)
		col2 := schema.NewColumn(inta, 6 /* maxDefLvl */, 1 /* maxRepLvl */)
		col3 := schema.NewColumn(inta, 5 /* maxDefLvl */, 2 /* maxRepLvl */)

		assert.True(t, col1.Equals(col1))
		assert.False(t, col1.Equals(col2))
		assert.False(t, col2.Equals(col3))
	})

	t.Run("BuildTree", func(t *testing.T) {
		inta := schema.NewInt32Node("a" /* name */, parquet.Repetitions.Required, -1 /* fieldID */)
		fields := schema.FieldList{inta}
		fields = append(fields,
			schema.NewInt64Node("b" /* name */, parquet.Repetitions.Optional, -1 /* fieldID */),
			schema.NewByteArrayNode("c" /* name */, parquet.Repetitions.Repeated, -1 /* fieldID */))

		item1 := schema.NewInt64Node("item1" /* name */, parquet.Repetitions.Required, -1 /* fieldID */)
		item2 := schema.NewBooleanNode("item2" /* name */, parquet.Repetitions.Optional, -1 /* fieldID */)
		item3 := schema.NewInt32Node("item3" /* name */, parquet.Repetitions.Repeated, -1 /* fieldID */)
		list := schema.MustGroup(schema.NewGroupNodeConverted("records" /* name */, parquet.Repetitions.Repeated, schema.FieldList{item1, item2, item3}, schema.ConvertedTypes.List, -1 /* fieldID */))
		bag := schema.MustGroup(schema.NewGroupNode("bag" /* name */, parquet.Repetitions.Optional, schema.FieldList{list}, -1 /* fieldID */))
		fields = append(fields, bag)

		sc := schema.MustGroup(schema.NewGroupNode("schema" /* name */, parquet.Repetitions.Repeated, fields, -1 /* fieldID */))
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

		nonColumnAlien := schema.NewInt32Node("alien" /* name */, parquet.Repetitions.Required, -1 /* fieldID */)
		nonColumnFamiliar := schema.NewInt32Node("a" /* name */, parquet.Repetitions.Repeated, -1 /* fieldID */)
		assert.Less(t, descr.ColumnIndexByNode(nonColumnAlien), 0)
		assert.Less(t, descr.ColumnIndexByNode(nonColumnFamiliar), 0)

		assert.Same(t, inta, descr.ColumnRoot(0))
		assert.Same(t, bag, descr.ColumnRoot(3))
		assert.Same(t, bag, descr.ColumnRoot(4))
		assert.Same(t, bag, descr.ColumnRoot(5))

		assert.Same(t, sc, descr.Root())
	})

	t.Run("HasRepeatedFields", func(t *testing.T) {
		inta := schema.NewInt32Node("a" /* name */, parquet.Repetitions.Required, -1 /* fieldID */)
		fields := schema.FieldList{inta}
		fields = append(fields,
			schema.NewInt64Node("b" /* name */, parquet.Repetitions.Optional, -1 /* fieldID */),
			schema.NewByteArrayNode("c" /* name */, parquet.Repetitions.Repeated, -1 /* fieldID */))

		sc := schema.MustGroup(schema.NewGroupNode("schema" /* name */, parquet.Repetitions.Repeated, fields, -1 /* fieldID */))
		descr := schema.NewSchema(sc)
		assert.True(t, descr.HasRepeatedFields())

		item1 := schema.NewInt64Node("item1" /* name */, parquet.Repetitions.Required, -1 /* fieldID */)
		item2 := schema.NewBooleanNode("item2" /* name */, parquet.Repetitions.Optional, -1 /* fieldID */)
		item3 := schema.NewInt32Node("item3" /* name */, parquet.Repetitions.Repeated, -1 /* fieldID */)
		list := schema.MustGroup(schema.NewGroupNodeConverted("records" /* name */, parquet.Repetitions.Repeated, schema.FieldList{item1, item2, item3}, schema.ConvertedTypes.List, -1 /* fieldID */))
		bag := schema.MustGroup(schema.NewGroupNode("bag" /* name */, parquet.Repetitions.Optional, schema.FieldList{list}, -1 /* fieldID */))
		fields = append(fields, bag)

		sc = schema.MustGroup(schema.NewGroupNode("schema" /* name */, parquet.Repetitions.Repeated, fields, -1 /* fieldID */))
		descr = schema.NewSchema(sc)
		assert.True(t, descr.HasRepeatedFields())

		itemKey := schema.NewInt64Node("key" /* name */, parquet.Repetitions.Required, -1 /* fieldID */)
		itemValue := schema.NewBooleanNode("value" /* name */, parquet.Repetitions.Optional, -1 /* fieldID */)
		sc = schema.MustGroup(schema.NewGroupNode("schema" /* name */, parquet.Repetitions.Repeated, append(fields, schema.FieldList{
			schema.MustGroup(schema.NewGroupNode("my_map" /* name */, parquet.Repetitions.Optional, schema.FieldList{
				schema.MustGroup(schema.NewGroupNodeConverted("map" /* name */, parquet.Repetitions.Repeated, schema.FieldList{itemKey, itemValue}, schema.ConvertedTypes.Map, -1 /* fieldID */)),
			}, -1 /* fieldID */)),
		}...), -1 /* fieldID */))
		descr = schema.NewSchema(sc)
		assert.True(t, descr.HasRepeatedFields())
	})
}

func ExamplePrintSchema() {
	fields := schema.FieldList{schema.NewInt32Node("a" /* name */, parquet.Repetitions.Required, 1 /* fieldID */)}
	item1 := schema.NewInt64Node("item1" /* name */, parquet.Repetitions.Optional, 4 /* fieldID */)
	item2 := schema.NewBooleanNode("item2" /* name */, parquet.Repetitions.Required, 5 /* fieldID */)
	list := schema.MustGroup(schema.NewGroupNodeConverted("b" /* name */, parquet.Repetitions.Repeated, schema.FieldList{item1, item2}, schema.ConvertedTypes.List, 3 /* fieldID */))
	bag := schema.MustGroup(schema.NewGroupNode("bag" /* name */, parquet.Repetitions.Optional, schema.FieldList{list}, 2 /* fieldID */))
	fields = append(fields, bag)

	fields = append(fields,
		schema.MustPrimitive(schema.NewPrimitiveNodeConverted("c" /* name */, parquet.Repetitions.Required, parquet.Types.Int32, schema.ConvertedTypes.Decimal, 0 /* type len */, 3 /* precision */, 2 /* scale */, 6 /* fieldID */)),
		schema.MustPrimitive(schema.NewPrimitiveNodeLogical("d" /* name */, parquet.Repetitions.Required, schema.NewDecimalLogicalType(10 /* precision */, 5 /* scale */), parquet.Types.Int64, -1 /* type len */, 7 /* fieldID */)))

	sc := schema.MustGroup(schema.NewGroupNode("schema" /* name */, parquet.Repetitions.Repeated, fields, 0 /* fieldID */))
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
		schema.MustPrimitive(schema.NewPrimitiveNodeLogical("map" /* name */, parquet.Repetitions.Required, schema.MapLogicalType{}, parquet.Types.Int64, -1 /* type len */, -1 /* fieldID */))
	}, "nested logical type on non-group node")

	assert.Panics(t, func() {
		schema.MustPrimitive(schema.NewPrimitiveNodeLogical("string" /* name */, parquet.Repetitions.Required, schema.StringLogicalType{}, parquet.Types.Boolean, -1 /* type len */, -1 /* fieldID */))
	}, "incompatible primitive type")

	assert.Panics(t, func() {
		schema.MustPrimitive(schema.NewPrimitiveNodeLogical("interval" /* name */, parquet.Repetitions.Required, schema.IntervalLogicalType{}, parquet.Types.FixedLenByteArray, 11 /* type len */, -1 /* fieldID */))
	}, "incompatible primitive length")

	assert.Panics(t, func() {
		schema.MustPrimitive(schema.NewPrimitiveNodeLogical("decimal" /* name */, parquet.Repetitions.Required, schema.NewDecimalLogicalType(16, 6), parquet.Types.Int32, -1 /* type len */, -1 /* fieldID */))
	}, "primitive too small for given precision")

	assert.Panics(t, func() {
		schema.MustPrimitive(schema.NewPrimitiveNodeLogical("uuid" /* name */, parquet.Repetitions.Required, schema.UUIDLogicalType{}, parquet.Types.FixedLenByteArray, 64 /* type len */, -1 /* fieldID */))
	}, "incompatible primitive length")

	assert.Panics(t, func() {
		schema.MustPrimitive(schema.NewPrimitiveNodeLogical("negative_len" /* name */, parquet.Repetitions.Required, schema.NoLogicalType{}, parquet.Types.FixedLenByteArray, -16 /* type len */, -1 /* fieldID */))
	}, "non-positive length for fixed length binary")

	assert.Panics(t, func() {
		schema.MustPrimitive(schema.NewPrimitiveNodeLogical("zero_len" /* name */, parquet.Repetitions.Required, schema.NoLogicalType{}, parquet.Types.FixedLenByteArray, 0 /* type len */, -1 /* fieldID */))
	}, "non-positive length for fixed length binary")

	assert.Panics(t, func() {
		schema.MustGroup(schema.NewGroupNodeLogical("list" /* name */, parquet.Repetitions.Repeated, schema.FieldList{}, schema.JSONLogicalType{}, -1 /* fieldID */))
	}, "non-nested logical type on group node")
}

func TestNullLogicalConvertsToNone(t *testing.T) {
	var (
		empty schema.LogicalType
		n     schema.Node
	)
	assert.NotPanics(t, func() {
		n = schema.MustPrimitive(schema.NewPrimitiveNodeLogical("value" /* name */, parquet.Repetitions.Required, empty, parquet.Types.Double, -1 /* type len */, -1 /* fieldID */))
	})
	assert.True(t, n.LogicalType().IsNone())
	assert.Equal(t, schema.ConvertedTypes.None, n.ConvertedType())
	assert.NotPanics(t, func() {
		n = schema.MustGroup(schema.NewGroupNodeLogical("items" /* name */, parquet.Repetitions.Repeated, schema.FieldList{}, empty, -1 /* fieldID */))
	})
	assert.True(t, n.LogicalType().IsNone())
	assert.Equal(t, schema.ConvertedTypes.None, n.ConvertedType())
}
