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

package array_test

import (
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/internal/types"
	"github.com/stretchr/testify/suite"
)

type ExtensionTypeTestSuite struct {
	suite.Suite
}

func (e *ExtensionTypeTestSuite) SetupTest() {
	e.NoError(arrow.RegisterExtensionType(types.NewUUIDType()))
}

func (e *ExtensionTypeTestSuite) TearDownTest() {
	if arrow.GetExtensionType("uuid") != nil {
		e.NoError(arrow.UnregisterExtensionType("uuid"))
	}
}

func (e *ExtensionTypeTestSuite) TestParametricEquals() {
	p1Type := types.NewParametric1Type(6)
	p2Type := types.NewParametric1Type(6)
	p3Type := types.NewParametric1Type(3)

	e.True(arrow.TypeEqual(p1Type, p2Type))
	e.False(arrow.TypeEqual(p1Type, p3Type))
}

func exampleParametric(mem memory.Allocator, dt arrow.DataType, vals []int32, valid []bool) arrow.Array {
	bldr := array.NewBuilder(mem, dt)
	defer bldr.Release()

	exb := bldr.(*array.ExtensionBuilder)
	sb := exb.StorageBuilder().(*array.Int32Builder)
	sb.AppendValues(vals, valid)

	return bldr.NewArray()
}

func (e *ExtensionTypeTestSuite) TestParametricArrays() {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(e.T(), 0)

	p1Type := types.NewParametric1Type(6)
	p1 := exampleParametric(pool, p1Type, []int32{-1, 1, 2, 3}, []bool{false, true, true, true})
	defer p1.Release()

	p2Type := types.NewParametric1Type(12)
	p2 := exampleParametric(pool, p2Type, []int32{2, -1, 3, 4}, []bool{true, false, true, true})
	defer p2.Release()

	p3Type := types.NewParametric2Type(2)
	p3 := exampleParametric(pool, p3Type, []int32{5, 6, 7, 8}, nil)
	defer p3.Release()

	p4Type := types.NewParametric2Type(3)
	p4 := exampleParametric(pool, p4Type, []int32{5, 6, 7, 9}, nil)
	defer p4.Release()

	rb := array.NewRecord(arrow.NewSchema([]arrow.Field{
		{Name: "f0", Type: p1Type, Nullable: true},
		{Name: "f1", Type: p2Type, Nullable: true},
		{Name: "f2", Type: p3Type, Nullable: true},
		{Name: "f3", Type: p4Type, Nullable: true},
	}, nil), []arrow.Array{p1, p2, p3, p4}, -1)
	defer rb.Release()

	e.True(array.RecordEqual(rb, rb))
}

func TestExtensionTypes(t *testing.T) {
	suite.Run(t, new(ExtensionTypeTestSuite))
}
