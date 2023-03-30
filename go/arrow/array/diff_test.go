package array_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

func TestDiff(t *testing.T) {
	cases := []struct {
		name          string
		base          string
		target        string
		dataType      arrow.DataType
		wantInsert    []bool
		wantRunLength []int64
	}{
		{
			name:          "trivial: empty",
			base:          `[]`,
			target:        `[]`,
			dataType:      arrow.PrimitiveTypes.Int32,
			wantInsert:    []bool{false},
			wantRunLength: []int64{0},
		},
		{
			name:          "trivial: nulls",
			base:          `[null, null]`,
			target:        `[null, null, null, null]`,
			dataType:      arrow.PrimitiveTypes.Int32,
			wantInsert:    []bool{false, true, true},
			wantRunLength: []int64{2, 0, 0},
		},
		{
			name:          "trivial: equal",
			base:          `[1, 2, 3]`,
			target:        `[1, 2, 3]`,
			dataType:      arrow.PrimitiveTypes.Int32,
			wantInsert:    []bool{false},
			wantRunLength: []int64{3},
		},
		{
			name:          "basics: insert one",
			base:          `[1, 2, null, 5]`,
			target:        `[1, 2, 3, null, 5]`,
			dataType:      arrow.PrimitiveTypes.Int32,
			wantInsert:    []bool{false, true},
			wantRunLength: []int64{2, 2},
		},
		{
			name:          "basics: delete one",
			base:          `[1, 2, 3, null, 5]`,
			target:        `[1, 2, null, 5]`,
			dataType:      arrow.PrimitiveTypes.Int32,
			wantInsert:    []bool{false, false},
			wantRunLength: []int64{2, 2},
		},
		{
			name:          "basics: change one",
			base:          `[1, 2, 3, null, 5]`,
			target:        `[1, 2, 23, null, 5]`,
			dataType:      arrow.PrimitiveTypes.Int32,
			wantInsert:    []bool{false, false, true},
			wantRunLength: []int64{2, 0, 2},
		},
		{
			name:          "basics: null out one",
			base:          `[1, 2, 3, null, 5]`,
			target:        `[1, 2, null, null, 5]`,
			dataType:      arrow.PrimitiveTypes.Int32,
			wantInsert:    []bool{false, false, true},
			wantRunLength: []int64{2, 1, 1},
		},
		{
			name:          "basics: append some",
			base:          `[1, 2, 3, null, 5]`,
			target:        `[1, 2, 3, null, 5, 6, 7, 8, 9]`,
			dataType:      arrow.PrimitiveTypes.Int32,
			wantInsert:    []bool{false, true, true, true, true},
			wantRunLength: []int64{5, 0, 0, 0, 0},
		},
		{
			name:          "basics: prepend some",
			base:          `[1, 2, 3, null, 5]`,
			target:        `[6, 4, 2, 0, 1, 2, 3, null, 5]`,
			dataType:      arrow.PrimitiveTypes.Int32,
			wantInsert:    []bool{false, true, true, true, true},
			wantRunLength: []int64{0, 0, 0, 0, 5},
		},
	}
	for _, tc := range cases {
		if tc.dataType == nil {
			t.Fatalf("test case %q has nil dataType", tc.name)
		}

		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			base, _, err := array.FromJSON(mem, tc.dataType, strings.NewReader(tc.base))
			if err != nil {
				t.Fatal(err)
			}
			defer base.Release()

			target, _, err := array.FromJSON(mem, tc.dataType, strings.NewReader(tc.target))
			if err != nil {
				t.Fatal(err)
			}
			defer target.Release()

			got, err := array.Diff(base, target, mem)
			if err != nil {
				t.Fatalf("got unexpected error %v", err)
			}
			defer got.Release()

			gotInsert := boolValues(got.Field(0).(*array.Boolean))
			gotRunLength := got.Field(1).(*array.Int64).Int64Values()
			if !reflect.DeepEqual(gotInsert, tc.wantInsert) {
				t.Errorf("Diff(\n  base=%v, \ntarget=%v\n) got insert %v, want %v", base, target, gotInsert, tc.wantInsert)
			}
			if !reflect.DeepEqual(gotRunLength, tc.wantRunLength) {
				t.Errorf("Diff(\n  base=%v, \ntarget=%v\n) got run length %v, want %v", base, target, gotRunLength, tc.wantRunLength)
			}
		})
	}
}

func boolValues(b *array.Boolean) []bool {
	ret := make([]bool, b.Len())
	for i := range ret {
		ret[i] = b.Value(i)
	}
	return ret
}
