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

package arrow_test

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/internal/testing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type BadExtensionType struct{}

func (BadExtensionType) ID() arrow.Type                           { return arrow.EXTENSION }
func (BadExtensionType) ArrayType() reflect.Type                  { return nil }
func (BadExtensionType) Name() string                             { return "bad" }
func (BadExtensionType) StorageType() arrow.DataType              { return arrow.Null }
func (BadExtensionType) ExtensionEquals(arrow.ExtensionType) bool { return false }
func (BadExtensionType) ExtensionName() string                    { return "bad" }
func (BadExtensionType) Serialize() string                        { return "" }
func (BadExtensionType) Deserialize(_ arrow.DataType, _ string) (arrow.ExtensionType, error) {
	return nil, nil
}

func TestMustEmbedBase(t *testing.T) {
	var ext interface{} = &BadExtensionType{}
	assert.Panics(t, func() {
		var _ arrow.ExtensionType = ext.(arrow.ExtensionType)
	})
}

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

func (e *ExtensionTypeTestSuite) TestExtensionType() {
	e.Nil(arrow.GetExtensionType("uuid-unknown"))
	e.NotNil(arrow.GetExtensionType("uuid"))

	e.Error(arrow.RegisterExtensionType(types.NewUUIDType()))
	e.Error(arrow.UnregisterExtensionType("uuid-unknown"))

	typ := types.NewUUIDType()
	e.Implements((*arrow.ExtensionType)(nil), typ)
	e.Equal(arrow.EXTENSION, typ.ID())
	e.Equal("extension", typ.Name())

	serialized := typ.Serialize()
	deserialized, err := typ.Deserialize(&arrow.FixedSizeBinaryType{ByteWidth: 16}, serialized)
	e.NoError(err)

	e.True(arrow.TypeEqual(deserialized.StorageType(), &arrow.FixedSizeBinaryType{ByteWidth: 16}))
	e.True(arrow.TypeEqual(deserialized, typ))
	e.False(arrow.TypeEqual(deserialized, &arrow.FixedSizeBinaryType{ByteWidth: 16}))
}

func TestExtensionTypes(t *testing.T) {
	suite.Run(t, new(ExtensionTypeTestSuite))
}
