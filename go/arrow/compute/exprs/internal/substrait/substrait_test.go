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

package substrait

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/compute"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/require"
	"github.com/substrait-io/substrait-go/expr"
	"github.com/substrait-io/substrait-go/proto"
	"google.golang.org/protobuf/encoding/protojson"
)

const extendedExpr = `{
	"extensionUris": [
	  {
		"extensionUriAnchor": 1,
		"uri": "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml"
	  }
	],
	"extensions": [
	  {
		"extensionFunction": {
		  "extensionUriReference": 1,
		  "functionAnchor": 2,
		  "name": "add"
		}
	  }
	],
	"base_schema": {
		"names": ["int"],
		"struct": {"types": [{"i32":{}}]}
	},
	"referred_expr": [{
		"output_names": ["sum"],
		"expression": {				
			"scalarFunction": {
				"functionReference": 2,
				"outputType": {"i32": {}},
				"arguments": [
					{"value": {"selection": {"directReference": {"structField": {"field": 0}}}}},
					{"value": {"literal": {"fp64": 10}}}
				]
			}
		}
	}]	
  }`

const simpleData = `[1, 2, 3, 4, 5, 6]`

func TestSubstraitExecute(t *testing.T) {
	var ex proto.ExtendedExpression
	require.NoError(t, protojson.Unmarshal([]byte(extendedExpr), &ex))

	expression, err := expr.ExtendedFromProto(&ex, nil)
	require.NoError(t, err)

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	arr, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(simpleData))
	require.NoError(t, err)

	val := compute.NewDatumWithoutOwning(arr)

	ctx := compute.WithAllocator(context.Background(), mem)
	extset := NewExtensionSetDefault(expression.Extensions)
	result, err := ExecuteScalarExpression(ctx, compute.ExecBatch{Values: []compute.Datum{val}, Len: int64(arr.Len())},
		expression.ReferredExpr[0].GetExpr(), extset)
	require.NoError(t, err)
	defer result.Release()

	out := result.(*compute.ArrayDatum).MakeArray()
	defer out.Release()
	fmt.Println(out)
}
