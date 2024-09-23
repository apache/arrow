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

package extensions_test

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/extensions"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

// testBool8Type minimally implements arrow.ExtensionType, but importantly does not implement array.CustomExtensionBuilder
// so it will fall back to the storage type's default builder.
type testBool8Type struct {
	arrow.ExtensionBase
}

func newTestBool8Type() *testBool8Type {
	return &testBool8Type{ExtensionBase: arrow.ExtensionBase{Storage: arrow.PrimitiveTypes.Int8}}
}

func (t *testBool8Type) ArrayType() reflect.Type                  { return reflect.TypeOf(testBool8Array{}) }
func (t *testBool8Type) ExtensionEquals(arrow.ExtensionType) bool { panic("unimplemented") }
func (t *testBool8Type) ExtensionName() string                    { panic("unimplemented") }
func (t *testBool8Type) Serialize() string                        { panic("unimplemented") }
func (t *testBool8Type) Deserialize(arrow.DataType, string) (arrow.ExtensionType, error) {
	panic("unimplemented")
}

type testBool8Array struct {
	array.ExtensionArrayBase
}

func TestUnmarshalExtensionTypes(t *testing.T) {
	logicalJSON := `[true,null,false,null,true]`
	storageJSON := `[1,null,0,null,1]`

	// extensions.Bool8Type implements array.CustomExtensionBuilder so we expect the array to be built with the custom builder
	arrCustomBuilder, _, err := array.FromJSON(memory.DefaultAllocator, extensions.NewBool8Type(), bytes.NewBufferString(logicalJSON))
	require.NoError(t, err)
	defer arrCustomBuilder.Release()
	require.Equal(t, 5, arrCustomBuilder.Len())

	// testBoolType falls back to the default builder for the storage type, so it cannot deserialize native booleans
	_, _, err = array.FromJSON(memory.DefaultAllocator, newTestBool8Type(), bytes.NewBufferString(logicalJSON))
	require.ErrorContains(t, err, "cannot unmarshal true into Go value of type int8")

	// testBoolType must build the array with the native storage type: Int8
	arrDefaultBuilder, _, err := array.FromJSON(memory.DefaultAllocator, newTestBool8Type(), bytes.NewBufferString(storageJSON))
	require.NoError(t, err)
	defer arrDefaultBuilder.Release()
	require.Equal(t, 5, arrDefaultBuilder.Len())

	arrBool8, ok := arrCustomBuilder.(*extensions.Bool8Array)
	require.True(t, ok)

	arrExt, ok := arrDefaultBuilder.(array.ExtensionArray)
	require.True(t, ok)

	// The physical layout of both arrays is identical
	require.True(t, array.Equal(arrBool8.Storage(), arrExt.Storage()))
}

// invalidExtensionType does not fully implement the arrow.ExtensionType interface, even though it embeds arrow.ExtensionBase
type invalidExtensionType struct {
	arrow.ExtensionBase
}

func newInvalidExtensionType() *invalidExtensionType {
	return &invalidExtensionType{ExtensionBase: arrow.ExtensionBase{Storage: arrow.BinaryTypes.String}}
}

func TestInvalidExtensionType(t *testing.T) {
	jsonStr := `["one","two","three"]`
	typ := newInvalidExtensionType()

	require.PanicsWithError(t, fmt.Sprintf("arrow/array: invalid extension type: %T", typ), func() {
		array.FromJSON(memory.DefaultAllocator, typ, bytes.NewBufferString(jsonStr))
	})
}

var (
	_ arrow.ExtensionType  = (*testBool8Type)(nil)
	_ array.ExtensionArray = (*testBool8Array)(nil)
)
