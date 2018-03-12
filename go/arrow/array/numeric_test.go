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

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestNewFloat64Data(t *testing.T) {
	exp := []float64{1.0, 2.0, 4.0, 8.0, 16.0}

	ad := array.NewData(arrow.PrimitiveTypes.Float64, len(exp), []*memory.Buffer{nil, memory.NewBufferBytes(arrow.Float64Traits.CastToBytes(exp))}, 0)
	fa := array.NewFloat64Data(ad)

	assert.Equal(t, len(exp), fa.Len(), "unexpected Len()")
	assert.Equal(t, exp, fa.Float64Values(), "unexpected Float64Values()")
}
