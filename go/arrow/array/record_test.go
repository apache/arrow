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
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestRecord(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)
	col1 := func() arrow.Array {
		ib := array.NewInt32Builder(mem)
		defer ib.Release()

		ib.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
		return ib.NewInt32Array()
	}()
	defer col1.Release()

	col2 := func() arrow.Array {
		b := array.NewFloat64Builder(mem)
		defer b.Release()

		b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
		return b.NewFloat64Array()
	}()
	defer col2.Release()

	col2_1 := func() arrow.Array {
		b := array.NewFloat64Builder(mem)
		defer b.Release()

		b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
		return b.NewFloat64Array()
	}()
	defer col2_1.Release()

	cols := []arrow.Array{col1, col2}
	rec := array.NewRecord(schema, cols, -1)
	defer rec.Release()

	rec.Retain()
	rec.Release()

	if got, want := rec.Schema(), schema; !got.Equal(want) {
		t.Fatalf("invalid schema: got=%#v, want=%#v", got, want)
	}

	if got, want := rec.NumRows(), int64(10); got != want {
		t.Fatalf("invalid number of rows: got=%d, want=%d", got, want)
	}
	if got, want := rec.NumCols(), int64(2); got != want {
		t.Fatalf("invalid number of columns: got=%d, want=%d", got, want)
	}
	if got, want := rec.Columns()[0], cols[0]; got != want {
		t.Fatalf("invalid column: got=%q, want=%q", got, want)
	}
	if got, want := rec.Column(0), cols[0]; got != want {
		t.Fatalf("invalid column: got=%q, want=%q", got, want)
	}
	if got, want := rec.ColumnName(0), schema.Field(0).Name; got != want {
		t.Fatalf("invalid column name: got=%q, want=%q", got, want)
	}
	if _, err := rec.SetColumn(0, col2_1); err == nil {
		t.Fatalf("expected an error")
	}
	newRec, err := rec.SetColumn(1, col2_1);
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer newRec.Release()
	if !reflect.DeepEqual(newRec.Column(1), col2_1) {
		t.Fatalf("invalid column: got=%q, want=%q", rec.Column(1), col2_1)
	}

	for _, tc := range []struct {
		i, j int64
		err  error
	}{
		{i: 0, j: 10, err: nil},
		{i: 1, j: 10, err: nil},
		{i: 1, j: 9, err: nil},
		{i: 0, j: 0, err: nil},
		{i: 1, j: 1, err: nil},
		{i: 10, j: 10, err: nil},
		{i: 1, j: 0, err: fmt.Errorf("arrow/array: index out of range")},
		{i: 1, j: 11, err: fmt.Errorf("arrow/array: index out of range")},
	} {
		t.Run(fmt.Sprintf("slice-%02d-%02d", tc.i, tc.j), func(t *testing.T) {
			if tc.err != nil {
				defer func() {
					e := recover()
					if e == nil {
						t.Fatalf("expected an error %q", tc.err)
					}
					switch err := e.(type) {
					case string:
						if err != tc.err.Error() {
							t.Fatalf("invalid panic message. got=%q, want=%q", err, tc.err)
						}
					case error:
						if err.Error() != tc.err.Error() {
							t.Fatalf("invalid panic message. got=%q, want=%q", err, tc.err)
						}
					default:
						t.Fatalf("invalid type for panic message: %T (err=%v)", err, err)
					}
				}()
			}
			sub := rec.NewSlice(tc.i, tc.j)
			defer sub.Release()

			if got, want := sub.NumRows(), tc.j-tc.i; got != want {
				t.Fatalf("invalid rec-slice number of rows: got=%d, want=%d", got, want)
			}
		})
	}

	for _, tc := range []struct {
		schema *arrow.Schema
		cols   []arrow.Array
		rows   int64
		err    error
	}{
		{
			schema: schema,
			cols:   nil,
			rows:   0,
		},
		{
			schema: schema,
			cols:   cols[:1],
			rows:   0,
			err:    fmt.Errorf("arrow/array: number of columns/fields mismatch"),
		},
		{
			schema: arrow.NewSchema(
				[]arrow.Field{
					{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
				},
				nil,
			),
			cols: cols,
			rows: 0,
			err:  fmt.Errorf("arrow/array: number of columns/fields mismatch"),
		},
		{
			schema: arrow.NewSchema(
				[]arrow.Field{
					{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
					{Name: "f2-f64", Type: arrow.PrimitiveTypes.Int32},
				},
				nil,
			),
			cols: cols,
			rows: 0,
			err:  fmt.Errorf(`arrow/array: column "f2-f64" type mismatch: got=float64, want=int32`),
		},
		{
			schema: schema,
			cols:   cols,
			rows:   11,
			err:    fmt.Errorf(`arrow/array: mismatch number of rows in column "f1-i32": got=10, want=11`),
		},
		{
			schema: schema,
			cols:   cols,
			rows:   10,
			err:    nil,
		},
		{
			schema: schema,
			cols:   cols,
			rows:   3,
			err:    nil,
		},
		{
			schema: schema,
			cols:   cols,
			rows:   0,
			err:    nil,
		},
	} {
		t.Run("", func(t *testing.T) {
			if tc.err != nil {
				defer func() {
					e := recover()
					if e == nil {
						t.Fatalf("expected an error %q", tc.err)
					}
					switch err := e.(type) {
					case string:
						if err != tc.err.Error() {
							t.Fatalf("invalid panic message. got=%q, want=%q", err, tc.err)
						}
					case error:
						if err.Error() != tc.err.Error() {
							t.Fatalf("invalid panic message. got=%q, want=%q", err, tc.err)
						}
					default:
						t.Fatalf("invalid type for panic message: %T (err=%v)", err, err)
					}
				}()
			}
			rec := array.NewRecord(tc.schema, tc.cols, tc.rows)
			defer rec.Release()
			if got, want := rec.NumRows(), tc.rows; got != want {
				t.Fatalf("invalid number of rows: got=%d, want=%d", got, want)
			}
		})
	}
}

func TestRecordReader(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)
	rec1 := func() arrow.Record {
		col1 := func() arrow.Array {
			ib := array.NewInt32Builder(mem)
			defer ib.Release()

			ib.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
			return ib.NewInt32Array()
		}()
		defer col1.Release()

		col2 := func() arrow.Array {
			b := array.NewFloat64Builder(mem)
			defer b.Release()

			b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
			return b.NewFloat64Array()
		}()
		defer col2.Release()

		cols := []arrow.Array{col1, col2}
		return array.NewRecord(schema, cols, -1)
	}()
	defer rec1.Release()

	rec2 := func() arrow.Record {
		col1 := func() arrow.Array {
			ib := array.NewInt32Builder(mem)
			defer ib.Release()

			ib.AppendValues([]int32{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, nil)
			return ib.NewInt32Array()
		}()
		defer col1.Release()

		col2 := func() arrow.Array {
			b := array.NewFloat64Builder(mem)
			defer b.Release()

			b.AppendValues([]float64{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, nil)
			return b.NewFloat64Array()
		}()
		defer col2.Release()

		cols := []arrow.Array{col1, col2}
		return array.NewRecord(schema, cols, -1)
	}()
	defer rec2.Release()

	recs := []arrow.Record{rec1, rec2}
	itr, err := array.NewRecordReader(schema, recs)
	if err != nil {
		t.Fatal(err)
	}
	defer itr.Release()

	itr.Retain()
	itr.Release()

	if got, want := itr.Schema(), schema; !got.Equal(want) {
		t.Fatalf("invalid schema. got=%#v, want=%#v", got, want)
	}

	n := 0
	for itr.Next() {
		n++
		if got, want := itr.Record(), recs[n-1]; !reflect.DeepEqual(got, want) {
			t.Fatalf("itr[%d], invalid record. got=%#v, want=%#v", n-1, got, want)
		}
	}
	if err := itr.Err(); err != nil {
		t.Fatalf("itr error: %#v", err)
	}

	if n != len(recs) {
		t.Fatalf("invalid number of iterations. got=%d, want=%d", n, len(recs))
	}

	for _, tc := range []struct {
		name   string
		schema *arrow.Schema
		err    error
	}{
		{
			name: "mismatch-name",
			schema: arrow.NewSchema(
				[]arrow.Field{
					{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
					{Name: "f2-XXX", Type: arrow.PrimitiveTypes.Float64},
				},
				nil,
			),
			err: fmt.Errorf("arrow/array: mismatch schema"),
		},
		{
			name: "mismatch-type",
			schema: arrow.NewSchema(
				[]arrow.Field{
					{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
					{Name: "f2-f64", Type: arrow.PrimitiveTypes.Int64},
				},
				nil,
			),
			err: fmt.Errorf("arrow/array: mismatch schema"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			itr, err := array.NewRecordReader(tc.schema, recs)
			if itr != nil {
				itr.Release()
			}
			if err == nil {
				t.Fatalf("expected an error: %v", tc.err)
			}
			if !assert.Equal(t, tc.err, err) {
				t.Fatalf("invalid error: got=%v, want=%v", err, tc.err)
			}
		})
	}
}

func TestRecordBuilder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	mapDt := arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)
	mapDt.KeysSorted = true
	mapDt.SetItemNullable(false)
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64},
			{Name: "map", Type: mapDt},
		},
		nil,
	)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Retain()
	b.Release()

	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{4, 5}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{1, 2, 3, 4, 5}, nil)
	mb := b.Field(2).(*array.MapBuilder)
	for i := 0; i < 5; i++ {
		mb.Append(true)

		if i%3 == 0 {
			mb.KeyBuilder().(*array.StringBuilder).AppendValues([]string{fmt.Sprint(i), "2", "3"}, nil)
			mb.ItemBuilder().(*array.StringBuilder).AppendValues([]string{"a", "b", "c"}, nil)
		}
	}

	rec := b.NewRecord()
	defer rec.Release()

	if got, want := rec.Schema(), schema; !got.Equal(want) {
		t.Fatalf("invalid schema: got=%#v, want=%#v", got, want)
	}

	if got, want := rec.NumRows(), int64(5); got != want {
		t.Fatalf("invalid number of rows: got=%d, want=%d", got, want)
	}
	if got, want := rec.NumCols(), int64(3); got != want {
		t.Fatalf("invalid number of columns: got=%d, want=%d", got, want)
	}
	if got, want := rec.ColumnName(0), schema.Field(0).Name; got != want {
		t.Fatalf("invalid column name: got=%q, want=%q", got, want)
	}
	if got, want := rec.Column(2).String(), `[{["0" "2" "3"] ["a" "b" "c"]} {[] []} {[] []} {["3" "2" "3"] ["a" "b" "c"]} {[] []}]`; got != want {
		t.Fatalf("invalid column name: got=%q, want=%q", got, want)
	}
}

type testMessage struct {
	Foo  *testMessageFoo
	Bars []*testMessageBar
}

func (m *testMessage) Reset() { *m = testMessage{} }

func (m *testMessage) GetFoo() *testMessageFoo {
	if m != nil {
		return m.Foo
	}
	return nil
}

func (m *testMessage) GetBars() []*testMessageBar {
	if m != nil {
		return m.Bars
	}
	return nil
}

type testMessageFoo struct {
	A int32
	B []uint32
}

func (m *testMessageFoo) Reset() { *m = testMessageFoo{} }

func (m *testMessageFoo) GetA() int32 {
	if m != nil {
		return m.A
	}
	return 0
}

func (m *testMessageFoo) GetB() []uint32 {
	if m != nil {
		return m.B
	}
	return nil
}

type testMessageBar struct {
	C int64
	D []uint64
}

func (m *testMessageBar) Reset() { *m = testMessageBar{} }

func (m *testMessageBar) GetC() int64 {
	if m != nil {
		return m.C
	}
	return 0
}

func (m *testMessageBar) GetD() []uint64 {
	if m != nil {
		return m.D
	}
	return nil
}

var testMessageSchema = arrow.NewSchema(
	[]arrow.Field{
		{Name: "foo", Type: arrow.StructOf(
			arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32},
			arrow.Field{Name: "b", Type: arrow.ListOf(
				arrow.PrimitiveTypes.Uint32,
			)},
		)},
		{Name: "bars", Type: arrow.ListOf(
			arrow.StructOf(
				arrow.Field{Name: "c", Type: arrow.PrimitiveTypes.Int64},
				arrow.Field{Name: "d", Type: arrow.ListOf(
					arrow.PrimitiveTypes.Uint64,
				)},
			),
		)},
	},
	nil,
)

func (m *testMessage) Fill(rec arrow.Record, row int) error {
	m.Reset()

	// foo
	if 0 < rec.NumCols() {
		src0 := rec.Column(0).Data()
		typedSrc0 := array.NewStructData(src0)
		defer typedSrc0.Release()
		if typedSrc0.IsValid(row) {
			m0 := &testMessageFoo{}
			{

				// a
				if 0 < typedSrc0.NumField() {
					src0_0 := typedSrc0.Field(0).Data()
					typedSrc0_0 := array.NewInt32Data(src0_0)
					defer typedSrc0_0.Release()
					m0.A = typedSrc0_0.Value(row)
				}

				// b
				if 1 < typedSrc0.NumField() {
					src0_1 := typedSrc0.Field(1).Data()
					listSrc0_1 := array.NewListData(src0_1)
					defer listSrc0_1.Release()
					if listSrc0_1.IsValid(row) {
						typedSrc0_1 := array.NewUint32Data(listSrc0_1.ListValues().Data())
						typedSrc0_1.Release()
						start0_1 := int(listSrc0_1.Offsets()[row])
						end0_1 := int(listSrc0_1.Offsets()[row+1])
						for row := start0_1; row < end0_1; row++ {
							m0.B = append(m0.B, typedSrc0_1.Value(row))
						}
					}
				}
			}
			m.Foo = m0
		}
	}

	// bars
	if 1 < rec.NumCols() {
		src1 := rec.Column(1).Data()
		listSrc1 := array.NewListData(src1)
		defer listSrc1.Release()
		if listSrc1.IsValid(row) {
			typedSrc1 := array.NewStructData(listSrc1.ListValues().Data())
			defer typedSrc1.Release()
			start1 := int(listSrc1.Offsets()[row])
			end1 := int(listSrc1.Offsets()[row+1])
			for row := start1; row < end1; row++ {
				if typedSrc1.IsValid(row) {
					m1 := &testMessageBar{}
					{

						// c
						if 0 < typedSrc1.NumField() {
							src1_0 := typedSrc1.Field(0).Data()
							typedSrc1_0 := array.NewInt64Data(src1_0)
							defer typedSrc1_0.Release()
							m1.C = typedSrc1_0.Value(row)
						}

						// d
						if 1 < typedSrc1.NumField() {
							src1_1 := typedSrc1.Field(1).Data()
							listSrc1_1 := array.NewListData(src1_1)
							defer listSrc1_1.Release()
							if listSrc1_1.IsValid(row) {
								typedSrc1_1 := array.NewUint64Data(listSrc1_1.ListValues().Data())
								defer typedSrc1_1.Release()
								start1_1 := int(listSrc1_1.Offsets()[row])
								end1_1 := int(listSrc1_1.Offsets()[row+1])
								for row := start1_1; row < end1_1; row++ {
									m1.D = append(m1.D, typedSrc1_1.Value(row))
								}
							}
						}
					}
					m.Bars = append(m.Bars, m1)
				} else {
					m.Bars = append(m.Bars, nil)
				}
			}
		}
	}
	return nil
}

func newTestMessageArrowRecordBuilder(mem memory.Allocator) *testMessageArrowRecordBuilder {
	return &testMessageArrowRecordBuilder{
		rb: array.NewRecordBuilder(mem, testMessageSchema),
	}
}

type testMessageArrowRecordBuilder struct {
	rb *array.RecordBuilder
}

func (b *testMessageArrowRecordBuilder) Build() arrow.Record {
	return b.rb.NewRecord()
}

func (b *testMessageArrowRecordBuilder) Release() {
	b.rb.Release()
}

func (b *testMessageArrowRecordBuilder) Append(m *testMessage) {

	// foo
	{
		builder0 := b.rb.Field(0)
		v0 := m.GetFoo()
		valueBuilder0 := builder0.(*array.StructBuilder)
		if v0 == nil {
			valueBuilder0.AppendNull()
		} else {
			valueBuilder0.Append(true)

			// a
			{
				v0_0 := v0.GetA()
				builder0_0 := valueBuilder0.FieldBuilder(0)
				valueBuilder0_0 := builder0_0.(*array.Int32Builder)
				valueBuilder0_0.Append(v0_0)
			}

			// b
			{
				v0_1 := v0.GetB()
				builder0_1 := valueBuilder0.FieldBuilder(1)
				listBuilder0_1 := builder0_1.(*array.ListBuilder)
				if len(v0_1) == 0 {
					listBuilder0_1.AppendNull()
				} else {
					listBuilder0_1.Append(true)
					valueBuilder0_1 := listBuilder0_1.ValueBuilder().(*array.Uint32Builder)
					for _, item := range v0_1 {
						valueBuilder0_1.Append(item)
					}
				}
			}
		}
	}

	// bars
	{
		builder1 := b.rb.Field(1)
		v1 := m.GetBars()
		listBuilder1 := builder1.(*array.ListBuilder)
		if len(v1) == 0 {
			listBuilder1.AppendNull()
		} else {
			listBuilder1.Append(true)
			valueBuilder1 := listBuilder1.ValueBuilder().(*array.StructBuilder)
			for _, item := range v1 {
				if item == nil {
					valueBuilder1.AppendNull()
				} else {
					valueBuilder1.Append(true)

					// c
					{
						v1_0 := item.GetC()
						builder1_0 := valueBuilder1.FieldBuilder(0)
						valueBuilder1_0 := builder1_0.(*array.Int64Builder)
						valueBuilder1_0.Append(v1_0)
					}

					// d
					{
						v1_1 := item.GetD()
						builder1_1 := valueBuilder1.FieldBuilder(1)
						listBuilder1_1 := builder1_1.(*array.ListBuilder)
						if len(v1_1) == 0 {
							listBuilder1_1.AppendNull()
						} else {
							listBuilder1_1.Append(true)
							valueBuilder1_1 := listBuilder1_1.ValueBuilder().(*array.Uint64Builder)
							for _, item := range v1_1 {
								valueBuilder1_1.Append(item)
							}
						}
					}
				}
			}
		}
	}
}

func TestRecordBuilderMessages(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	b := newTestMessageArrowRecordBuilder(mem)
	defer b.Release()

	var msgs []*testMessage
	for i := 0; i < 1000; i++ {
		msg := &testMessage{
			Foo: &testMessageFoo{
				A: int32(i),
				B: []uint32{2, 3, 4, 5, 6, 7, 8, 9},
			},
			Bars: []*testMessageBar{
				{
					C: 11,
					D: []uint64{12, 13, 14},
				},
				{
					C: 15,
					D: []uint64{16, 17, 18, 19},
				},
				nil,
				{
					C: 20,
					D: []uint64{21},
				},
			},
		}
		msgs = append(msgs, msg)
		b.Append(msg)
	}

	rec := b.Build()
	defer rec.Release()

	var got testMessage
	for i := 0; i < 1000; i++ {
		got.Fill(rec, i)
		if !reflect.DeepEqual(&got, msgs[i]) {
			t.Fatalf("row[%d], invalid record. got=%#v, want=%#v", i, &got, msgs[i])
		}
	}
}
