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

package compute_test

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/compute"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/arrow/scalar"
	"github.com/stretchr/testify/require"
)

func checkScalarBinary(t *testing.T, fn string, left, right, expected compute.Datum, opts compute.FunctionOptions) {
	checkScalar(t, fn, []compute.Datum{left, right}, expected, opts)
}

func checkBooleanScalarArrayBinary(t *testing.T, ctx context.Context, funcName string, array compute.Datum) {
	mem := compute.GetAllocator(ctx)
	for _, sc := range []scalar.Scalar{scalar.MakeNullScalar(arrow.FixedWidthTypes.Boolean), scalar.NewBooleanScalar(true), scalar.NewBooleanScalar(false)} {
		constantArr, err := scalar.MakeArrayFromScalar(sc, int(array.Len()), mem)
		defer constantArr.Release()

		require.NoError(t, err)
		expected, err := compute.CallFunction(ctx, funcName, nil, &compute.ArrayDatum{Value: constantArr.Data()}, array)
		require.NoError(t, err)
		defer expected.Release()

		checkScalar(t, funcName, []compute.Datum{compute.NewDatum(sc), array}, expected, nil)

		expected, err = compute.CallFunction(ctx, funcName, nil, array, &compute.ArrayDatum{Value: constantArr.Data()})
		require.NoError(t, err)
		defer expected.Release()
		checkScalar(t, funcName, []compute.Datum{array, compute.NewDatum(sc)}, expected, nil)
	}
}

func TestBooleanKernels(t *testing.T) {
	tests := []struct {
		fn           string
		expectedJSON string
		commutative  bool
	}{
		{"and", `[true, false, null, false, null, null]`, true},
		{"or", `[true, true, null, false, null, null]`, true},
		{"xor", `[false, true, null, false, null, null]`, true},
		{"and_not", `[false, true, null, false, false, null, null, null, null]`, false},
	}

	for _, tt := range tests {
		t.Run(tt.fn, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			var (
				leftJSON  = `[true, true, true, false, false, null]`
				rightJSON = `[true, false, null, false, null, null]`
			)

			if !tt.commutative {
				leftJSON = `[true, true, true, false, false, false, null, null, null]`
				rightJSON = `[true, false, null, true, false, null, true, false, null]`
			}

			left, _, _ := array.FromJSON(mem, arrow.FixedWidthTypes.Boolean,
				strings.NewReader(leftJSON))
			defer left.Release()
			right, _, _ := array.FromJSON(mem, arrow.FixedWidthTypes.Boolean,
				strings.NewReader(rightJSON))
			defer right.Release()
			exp, _, _ := array.FromJSON(mem, arrow.FixedWidthTypes.Boolean, strings.NewReader(tt.expectedJSON))
			defer exp.Release()

			checkScalarBinary(t, tt.fn, &compute.ArrayDatum{Value: left.Data()}, &compute.ArrayDatum{Value: right.Data()}, &compute.ArrayDatum{Value: exp.Data()}, nil)
			ctx := compute.WithAllocator(context.Background(), mem)
			checkBooleanScalarArrayBinary(t, ctx, tt.fn, &compute.ArrayDatum{Value: left.Data()})
		})
	}
}

func TestBooleanKleeneKernels(t *testing.T) {
	tests := []struct {
		fn           string
		expectedJSON []string
		commutative  bool
	}{
		{"and_kleene", []string{`[true, false, null, false, false, null]`, `[true, false, false, null, false]`, `[true, false, false, false]`}, true},
		{"or_kleene", []string{`[true, true, true, false, null, null]`, `[true, true, false, true, null]`, `[true, true, false, true]`}, true},
		{"and_not_kleene", []string{`[false, true, null, false, false, false, false, null, null]`, `[false, true, false, false]`}, false},
	}

	for _, tt := range tests {
		t.Run(tt.fn, func(t *testing.T) {
			var (
				leftJSON  = make([]string, len(tt.expectedJSON))
				rightJSON = make([]string, len(tt.expectedJSON))
			)

			if tt.commutative {
				leftJSON[0] = `[true, true, true, false, false, null]`
				rightJSON[0] = `[true, false, null, false, null, null]`
				leftJSON[1] = `[true, true, false, null, null]`
				rightJSON[1] = `[true, false, false, true, false]`
				leftJSON[2] = `[true, true, false, true]`
				rightJSON[2] = `[true, false, false, false]`
			} else {
				leftJSON[0] = `[true, true, true, false, false, false, null, null, null]`
				rightJSON[0] = `[true, false, null, true, false, null, true, false, null]`
				leftJSON[1] = `[true, true, false, false]`
				rightJSON[1] = `[true, false, true, false]`
			}

			for i := range tt.expectedJSON {
				func() {
					mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
					defer mem.AssertSize(t, 0)

					left, _, _ := array.FromJSON(mem, arrow.FixedWidthTypes.Boolean,
						strings.NewReader(leftJSON[i]))
					defer left.Release()
					right, _, _ := array.FromJSON(mem, arrow.FixedWidthTypes.Boolean,
						strings.NewReader(rightJSON[i]))
					defer right.Release()
					exp, _, _ := array.FromJSON(mem, arrow.FixedWidthTypes.Boolean, strings.NewReader(tt.expectedJSON[i]))
					defer exp.Release()

					checkScalarBinary(t, tt.fn, &compute.ArrayDatum{Value: left.Data()}, &compute.ArrayDatum{Value: right.Data()}, &compute.ArrayDatum{Value: exp.Data()}, nil)
					ctx := compute.WithAllocator(context.Background(), mem)
					checkBooleanScalarArrayBinary(t, ctx, tt.fn, &compute.ArrayDatum{Value: left.Data()})
				}()
			}
		})
	}
}
