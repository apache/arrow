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

package exprs

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/compute"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	boringArrowSchema = arrow.NewSchema([]arrow.Field{
		{Name: "bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "i32_req", Type: arrow.PrimitiveTypes.Int32},
		{Name: "u32", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "f32_req", Type: arrow.PrimitiveTypes.Float32},
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "date32", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "str", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "bin", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)
)

func TestMakeExecBatch(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	const numRows = 3
	var (
		ctx         = compute.WithAllocator(context.Background(), mem)
		i32, _, _   = array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[1, 2, 3]`))
		f32, _, _   = array.FromJSON(mem, arrow.PrimitiveTypes.Float32, strings.NewReader(`[1.5, 2.25, 3.125]`))
		empty, _, _ = array.RecordFromJSON(mem, boringArrowSchema, strings.NewReader(`[]`))
	)
	defer i32.Release()
	defer f32.Release()

	getField := func(n string) arrow.Field {
		f, _ := boringArrowSchema.FieldsByName(n)
		return f[0]
	}

	tests := []struct {
		name  string
		batch arrow.Record
	}{
		{"empty", empty},
		{"subset", array.NewRecord(arrow.NewSchema([]arrow.Field{getField("i32"), getField("f32")}, nil),
			[]arrow.Array{i32, f32}, numRows)},
		{"flipped subset", array.NewRecord(arrow.NewSchema([]arrow.Field{getField("f32"), getField("i32")}, nil),
			[]arrow.Array{f32, i32}, numRows)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer tt.batch.Release()
			batch, err := makeExecBatch(ctx, boringArrowSchema, compute.NewDatumWithoutOwning(tt.batch))
			require.NoError(t, err)
			require.Equal(t, tt.batch.NumRows(), batch.Len)

			defer func() {
				for _, v := range batch.Values {
					v.Release()
				}
			}()

			for i, field := range boringArrowSchema.Fields() {
				typ := batch.Values[i].(compute.ArrayLikeDatum).Type()
				assert.Truef(t, arrow.TypeEqual(typ, field.Type),
					"expected: %s\ngot: %s", field.Type, typ)

				idxes := tt.batch.Schema().FieldIndices(field.Name)
				if batch.Values[i].Kind() == compute.KindScalar {
					assert.False(t, batch.Values[i].(*compute.ScalarDatum).Value.IsValid(),
						"null placeholder should be injected")
					assert.Len(t, idxes, 0, "should only happen when column isn't found")
				} else {
					col := tt.batch.Column(idxes[0])
					val := batch.Values[i].(*compute.ArrayDatum).MakeArray()
					defer val.Release()

					assert.Truef(t, array.Equal(col, val), "expected: %s\ngot: %s", col, val)
				}
			}
		})
	}
}
