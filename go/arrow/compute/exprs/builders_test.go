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

//go:build go1.18

package exprs_test

import (
	"testing"

	"github.com/apache/arrow/go/v12/arrow/compute/exprs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/substrait-io/substrait-go/expr"
)

func TestNewScalarFunc(t *testing.T) {
	fn, err := exprs.NewScalarCall("add:i32_i32", nil,
		expr.NewPrimitiveLiteral(int32(1), false),
		expr.NewPrimitiveLiteral(int32(10), false))
	require.NoError(t, err)

	assert.Equal(t, "add:i32_i32(i32(1), i32(10)) => i32", fn.String())
}
