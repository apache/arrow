package array_test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

type diffTestCase struct {
	dataType arrow.DataType

	baseJSON      string
	targetJSON    string
	wantInsert    []bool
	wantRunLength []int64
}

func (s *diffTestCase) check(t *testing.T) {
	t.Helper()

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	base, _, err := array.FromJSON(mem, s.dataType, strings.NewReader(s.baseJSON))
	if err != nil {
		t.Fatal(err)
	}
	defer base.Release()

	target, _, err := array.FromJSON(mem, s.dataType, strings.NewReader(s.targetJSON))
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
	if !reflect.DeepEqual(gotInsert, s.wantInsert) {
		t.Errorf("Diff(\n  base=%v, \ntarget=%v\n) got insert %v, want %v", base, target, gotInsert, s.wantInsert)
	}
	if !reflect.DeepEqual(gotRunLength, s.wantRunLength) {
		t.Errorf("Diff(\n  base=%v, \ntarget=%v\n) got run length %v, want %v", base, target, gotRunLength, s.wantRunLength)
	}
}

func boolValues(b *array.Boolean) []bool {
	ret := make([]bool, b.Len())
	for i := range ret {
		ret[i] = b.Value(i)
	}
	return ret
}

func TestDiff_Trivial(t *testing.T) {
	cases := []struct {
		name          string
		base          string
		target        string
		wantInsert    []bool
		wantRunLength []int64
	}{
		{
			name:          "empty",
			base:          `[]`,
			target:        `[]`,
			wantInsert:    []bool{false},
			wantRunLength: []int64{0},
		},
		{
			name:          "nulls",
			base:          `[null, null]`,
			target:        `[null, null, null, null]`,
			wantInsert:    []bool{false, true, true},
			wantRunLength: []int64{2, 0, 0},
		},
		{
			name:          "equal",
			base:          `[1, 2, 3]`,
			target:        `[1, 2, 3]`,
			wantInsert:    []bool{false},
			wantRunLength: []int64{3},
		},
	}
	for _, tc := range cases {
		d := diffTestCase{
			dataType:      arrow.PrimitiveTypes.Int32,
			baseJSON:      tc.base,
			targetJSON:    tc.target,
			wantInsert:    tc.wantInsert,
			wantRunLength: tc.wantRunLength,
		}
		t.Run(tc.name, d.check)
	}
}

func TestDiff_Basics(t *testing.T) {
	cases := []struct {
		name          string
		base          string
		target        string
		wantInsert    []bool
		wantRunLength []int64
	}{
		{
			name:          "insert one",
			base:          `[1, 2, null, 5]`,
			target:        `[1, 2, 3, null, 5]`,
			wantInsert:    []bool{false, true},
			wantRunLength: []int64{2, 2},
		},
		{
			name:          "delete one",
			base:          `[1, 2, 3, null, 5]`,
			target:        `[1, 2, null, 5]`,
			wantInsert:    []bool{false, false},
			wantRunLength: []int64{2, 2},
		},
		{
			name:          "change one",
			base:          `[1, 2, 3, null, 5]`,
			target:        `[1, 2, 23, null, 5]`,
			wantInsert:    []bool{false, false, true},
			wantRunLength: []int64{2, 0, 2},
		},
		{
			name:          "null out one",
			base:          `[1, 2, 3, null, 5]`,
			target:        `[1, 2, null, null, 5]`,
			wantInsert:    []bool{false, false, true},
			wantRunLength: []int64{2, 1, 1},
		},
		{
			name:          "append some",
			base:          `[1, 2, 3, null, 5]`,
			target:        `[1, 2, 3, null, 5, 6, 7, 8, 9]`,
			wantInsert:    []bool{false, true, true, true, true},
			wantRunLength: []int64{5, 0, 0, 0, 0},
		},
		{
			name:          "prepend some",
			base:          `[1, 2, 3, null, 5]`,
			target:        `[6, 4, 2, 0, 1, 2, 3, null, 5]`,
			wantInsert:    []bool{false, true, true, true, true},
			wantRunLength: []int64{0, 0, 0, 0, 5},
		},
	}
	for _, tc := range cases {
		d := diffTestCase{
			dataType:      arrow.PrimitiveTypes.Int32,
			baseJSON:      tc.base,
			targetJSON:    tc.target,
			wantInsert:    tc.wantInsert,
			wantRunLength: tc.wantRunLength,
		}
		t.Run(tc.name, d.check)
	}
}

func TestDiff_BasicsWithBooleans(t *testing.T) {
	cases := []struct {
		name          string
		base          string
		target        string
		wantInsert    []bool
		wantRunLength []int64
	}{
		{
			name:          "insert one",
			base:          `[true, true, true]`,
			target:        `[true, false, true, true]`,
			wantInsert:    []bool{false, true},
			wantRunLength: []int64{1, 2},
		},
		{
			name:          "delete one",
			base:          `[true, false, true, true]`,
			target:        `[true, true, true]`,
			wantInsert:    []bool{false, false},
			wantRunLength: []int64{1, 2},
		},
		{
			name:          "change one",
			base:          `[false, false, true]`,
			target:        `[true, false, true]`,
			wantInsert:    []bool{false, false, true},
			wantRunLength: []int64{0, 0, 2},
		},
		{
			name:          "null out one",
			base:          `[true, false, true]`,
			target:        `[true, false, null]`,
			wantInsert:    []bool{false, false, true},
			wantRunLength: []int64{2, 0, 0},
		},
	}
	for _, tc := range cases {
		d := diffTestCase{
			dataType:      &arrow.BooleanType{},
			baseJSON:      tc.base,
			targetJSON:    tc.target,
			wantInsert:    tc.wantInsert,
			wantRunLength: tc.wantRunLength,
		}
		t.Run(tc.name, d.check)
	}
}

func TestDiff_BasicsWithStrings(t *testing.T) {
	cases := []struct {
		name          string
		base          string
		target        string
		wantInsert    []bool
		wantRunLength []int64
	}{
		{
			name:          "insert one",
			base:          `["give", "a", "break"]`,
			target:        `["give", "me", "a", "break"]`,
			wantInsert:    []bool{false, true},
			wantRunLength: []int64{1, 2},
		},
		{
			name:          "delete one",
			base:          `["give", "me", "a", "break"]`,
			target:        `["give", "a", "break"]`,
			wantInsert:    []bool{false, false},
			wantRunLength: []int64{1, 2},
		},
		{
			name:          "change one",
			base:          `["give", "a", "break"]`,
			target:        `["gimme", "a", "break"]`,
			wantInsert:    []bool{false, false, true},
			wantRunLength: []int64{0, 0, 2},
		},
		{
			name:          "null out one",
			base:          `["give", "a", "break"]`,
			target:        `["give", "a", null]`,
			wantInsert:    []bool{false, false, true},
			wantRunLength: []int64{2, 0, 0},
		},
	}
	for _, tc := range cases {
		d := diffTestCase{
			dataType:      &arrow.StringType{},
			baseJSON:      tc.base,
			targetJSON:    tc.target,
			wantInsert:    tc.wantInsert,
			wantRunLength: tc.wantRunLength,
		}
		t.Run(tc.name, d.check)
	}
}

func TestDiff_BasicsWithLists(t *testing.T) {
	cases := []struct {
		name          string
		base          string
		target        string
		wantInsert    []bool
		wantRunLength []int64
	}{
		{
			name:          "insert one",
			base:          `[[2, 3, 1], [], [13]]`,
			target:        `[[2, 3, 1], [5, 9], [], [13]]`,
			wantInsert:    []bool{false, true},
			wantRunLength: []int64{1, 2},
		},
		{
			name:          "delete one",
			base:          `[[2, 3, 1], [5, 9], [], [13]]`,
			target:        `[[2, 3, 1], [], [13]]`,
			wantInsert:    []bool{false, false},
			wantRunLength: []int64{1, 2},
		},
		{
			name:          "change one",
			base:          `[[2, 3, 1], [], [13]]`,
			target:        `[[3, 3, 3], [], [13]]`,
			wantInsert:    []bool{false, false, true},
			wantRunLength: []int64{0, 0, 2},
		},
		{
			name:          "null out one",
			base:          `[[2, 3, 1], [], [13]]`,
			target:        `[[2, 3, 1], [], null]`,
			wantInsert:    []bool{false, false, true},
			wantRunLength: []int64{2, 0, 0},
		},
	}
	for _, tc := range cases {
		d := diffTestCase{
			dataType:      arrow.ListOf(arrow.PrimitiveTypes.Int32),
			baseJSON:      tc.base,
			targetJSON:    tc.target,
			wantInsert:    tc.wantInsert,
			wantRunLength: tc.wantRunLength,
		}
		t.Run(tc.name, d.check)
	}
}

func TestDiff_BasicsWithStructs(t *testing.T) {
	cases := []struct {
		name          string
		base          string
		target        string
		wantInsert    []bool
		wantRunLength []int64
	}{
		{
			name:          "insert one",
			base:          `[{"foo": "!", "bar": 3}, {}, {"bar": 13}]`,
			target:        `[{"foo": "!", "bar": 3}, {"foo": "?"}, {}, {"bar": 13}]`,
			wantInsert:    []bool{false, true},
			wantRunLength: []int64{1, 2},
		},
		{
			name:          "delete one",
			base:          `[{"foo": "!", "bar": 3}, {"foo": "?"}, {}, {"bar": 13}]`,
			target:        `[{"foo": "!", "bar": 3}, {}, {"bar": 13}]`,
			wantInsert:    []bool{false, false},
			wantRunLength: []int64{1, 2},
		},
		{
			name:          "change one",
			base:          `[{"foo": "!", "bar": 3}, {}, {"bar": 13}]`,
			target:        `[{"foo": "!", "bar": 2}, {}, {"bar": 13}]`,
			wantInsert:    []bool{false, false, true},
			wantRunLength: []int64{0, 0, 2},
		},
		{
			name:          "null out one",
			base:          `[{"foo": "!", "bar": 3}, {}, {"bar": 13}]`,
			target:        `[{"foo": "!", "bar": 3}, {}, null]`,
			wantInsert:    []bool{false, false, true},
			wantRunLength: []int64{2, 0, 0},
		},
	}
	for _, tc := range cases {
		f1 := arrow.Field{Name: "foo", Type: arrow.BinaryTypes.String, Nullable: true}
		f2 := arrow.Field{Name: "bar", Type: arrow.PrimitiveTypes.Int32, Nullable: true}
		d := diffTestCase{
			dataType:      arrow.StructOf(f1, f2),
			baseJSON:      tc.base,
			targetJSON:    tc.target,
			wantInsert:    tc.wantInsert,
			wantRunLength: tc.wantRunLength,
		}
		t.Run(tc.name, d.check)
	}
}

func TestDiff_Random(t *testing.T) {
	rng := rand.New(rand.NewSource(0xdeadbeef))
	for i := 0; i < 100; i++ {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			testRandomCase(t, rng)
		})
	}
}

func testRandomCase(t *testing.T, rng *rand.Rand) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dataType := arrow.PrimitiveTypes.Int32

	baseValues := make([]int32, rng.Intn(10))
	for i := range baseValues {
		baseValues[i] = rng.Int31()
	}
	baseJSON, err := json.Marshal(baseValues)
	if err != nil {
		t.Fatal(err)
	}

	targetValues := make([]int32, rng.Intn(10))
	for i := range targetValues {
		// create runs with some probability
		if rng.Intn(2) == 0 && len(baseValues) > 0 {
			targetValues[i] = baseValues[rng.Intn(len(baseValues))]
		} else {
			targetValues[i] = rng.Int31()
		}
	}
	targetJSON, err := json.Marshal(targetValues)
	if err != nil {
		t.Fatal(err)
	}

	base, _, err := array.FromJSON(mem, dataType, strings.NewReader(string(baseJSON)))
	if err != nil {
		t.Fatal(err)
	}
	defer base.Release()

	target, _, err := array.FromJSON(mem, dataType, strings.NewReader(string(targetJSON)))
	if err != nil {
		t.Fatal(err)
	}
	defer target.Release()

	got, err := array.Diff(base, target, mem)
	if err != nil {
		t.Fatalf("got unexpected error %v", err)
	}
	defer got.Release()

	validateEditScript(t, got, base, target)
}

// validateEditScript checks that the edit script produces target when applied to base.
func validateEditScript(t *testing.T, edits *array.Struct, base, target arrow.Array) {
	t.Helper()

	inserts := boolValues(edits.Field(0).(*array.Boolean))
	runLengths := edits.Field(1).(*array.Int64).Int64Values()

	if len(runLengths) == 0 {
		t.Fatalf("edit script has run length of zero")
	}
	if len(runLengths) != len(inserts) {
		t.Fatalf("edit script has %d run lengths but %d insert flags", len(runLengths), len(inserts))
	}

	baseIndex := int64(0)
	targetIndex := int64(0)
	for i := 0; i < len(runLengths); i++ {
		if i > 0 {
			if inserts[i] {
				targetIndex++
			} else {
				baseIndex++
			}
		}
		for j := int64(0); j < runLengths[i]; j++ {
			if !array.SliceEqual(base, baseIndex, baseIndex+1, target, targetIndex, targetIndex+1) {
				t.Fatalf("edit script (inserts=%v, runLengths=%v) when applied to base %v does not produce target %v", inserts, runLengths, base, target)
			}
			baseIndex += 1
			targetIndex += 1
		}
	}
	if baseIndex != int64(base.Len()) || targetIndex != int64(target.Len()) {
		t.Fatalf("edit script (inserts=%v, runLengths=%v) when applied to base %v does not produce target %v", inserts, runLengths, base, target)
	}
}

type diffStringTestCase struct {
	dataType arrow.DataType

	name       string
	baseJSON   string
	targetJSON string
	want       string
}

func (s *diffStringTestCase) check(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	base, _, err := array.FromJSON(mem, s.dataType, strings.NewReader(s.baseJSON))
	if err != nil {
		t.Fatal(err)
	}
	defer base.Release()

	target, _, err := array.FromJSON(mem, s.dataType, strings.NewReader(s.targetJSON))
	if err != nil {
		t.Fatal(err)
	}
	defer target.Release()

	got, err := array.DiffString(base, target, mem)
	if err != nil {
		t.Fatalf("got unexpected error %v", err)
	}

	if got != s.want {
		t.Errorf("got:\n%v\n, want:\n%v", got, s.want)
	}
}

func TestDiffString(t *testing.T) {
	cases := []diffStringTestCase{
		{
			name:       "no changes",
			dataType:   arrow.BinaryTypes.String,
			baseJSON:   `["give", "me", "a", "break"]`,
			targetJSON: `["give", "me", "a", "break"]`,
			want:       ``,
		},
		{
			name:       "insert one",
			dataType:   arrow.BinaryTypes.String,
			baseJSON:   `["give", "a", "break"]`,
			targetJSON: `["give", "me", "a", "break"]`,
			want: `@@ -1, +1 @@
+"me"
`,
		},
		{
			name:       "delete one",
			dataType:   arrow.BinaryTypes.String,
			baseJSON:   `["give", "me", "a", "break"]`,
			targetJSON: `["give", "a", "break"]`,
			want: `@@ -1, +1 @@
-"me"
`,
		},
		{
			name:       "change one",
			dataType:   arrow.BinaryTypes.String,
			baseJSON:   `["give", "a", "break"]`,
			targetJSON: `["gimme", "a", "break"]`,
			want: `@@ -0, +0 @@
-"give"
+"gimme"
`,
		},
		{
			name:       "null out one",
			dataType:   arrow.BinaryTypes.String,
			baseJSON:   `["give", "a", "break"]`,
			targetJSON: `["give", "a", null]`,
			want: `@@ -2, +2 @@
-"break"
+null
`,
		},
		{
			name:       "strings with escaped chars",
			dataType:   arrow.BinaryTypes.String,
			baseJSON:   `["newline:\\n", "quote:'", "backslash:\\\\"]`,
			targetJSON: `["newline:\\n", "tab:\\t", "quote:\\\"", "backslash:\\\\"]`,
			want: `@@ -1, +1 @@
-"quote:'"
+"tab:\\t"
+"quote:\\\""
`,
		},
		//		{
		//			name:       "date32",
		//			dataType:   arrow.PrimitiveTypes.Date32,
		//			baseJSON:   `[0, 1, 2, 31, 4]`,
		//			targetJSON: `[0, 1, 31, 2, 4]`,
		//			want: `@@ -2, +2 @@
		//-1970-01-03
		//@@ -4, +3 @@
		//+1970-01-03
		//`,
		//		},
		{
			name:       "string",
			dataType:   arrow.BinaryTypes.String,
			baseJSON:   `["h", "l", "l", "o", "o"]`,
			targetJSON: `["h", "e", "l", "l", "o", "0"]`,
			want: `@@ -1, +1 @@
+"e"
@@ -4, +5 @@
-"o"
+"0"
`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, tc.check)
	}
}
