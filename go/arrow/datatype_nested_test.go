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

package arrow

import (
	"reflect"
	"testing"
)

func TestListOf(t *testing.T) {
	for _, tc := range []DataType{
		FixedWidthTypes.Boolean,
		PrimitiveTypes.Int8,
		PrimitiveTypes.Int16,
		PrimitiveTypes.Int32,
		PrimitiveTypes.Int64,
		PrimitiveTypes.Uint8,
		PrimitiveTypes.Uint16,
		PrimitiveTypes.Uint32,
		PrimitiveTypes.Uint64,
		PrimitiveTypes.Float32,
		PrimitiveTypes.Float64,
		ListOf(PrimitiveTypes.Int32),
		FixedSizeListOf(10, PrimitiveTypes.Int32),
		StructOf(),
	} {
		t.Run(tc.Name(), func(t *testing.T) {
			got := ListOf(tc)
			want := &ListType{elem: Field{Name: "item", Type: tc, Nullable: true}}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%#v, want=%#v", got, want)
			}

			if got, want := got.Name(), "list"; got != want {
				t.Fatalf("got=%q, want=%q", got, want)
			}

			if got, want := got.ID(), LIST; got != want {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			if got, want := got.Elem(), tc; got != want {
				t.Fatalf("got=%v, want=%v", got, want)
			}
		})
	}

	for _, dtype := range []DataType{
		nil,
		// (*Int32Type)(nil), // FIXME(sbinet): should we make sure this is actually caught?
		// (*ListType)(nil), // FIXME(sbinet): should we make sure this is actually caught?
		// (*StructType)(nil), // FIXME(sbinet): should we make sure this is actually caught?
	} {
		t.Run("invalid", func(t *testing.T) {
			defer func() {
				e := recover()
				if e == nil {
					t.Fatalf("test should have panicked but did not")
				}
			}()

			_ = ListOf(dtype)
		})
	}
}

func TestStructOf(t *testing.T) {
	for _, tc := range []struct {
		fields []Field
		want   DataType
	}{
		{
			fields: nil,
			want:   &StructType{fields: nil, index: nil},
		},
		{
			fields: []Field{{Name: "f1", Type: PrimitiveTypes.Int32}},
			want: &StructType{
				fields: []Field{{Name: "f1", Type: PrimitiveTypes.Int32}},
				index:  map[string]int{"f1": 0},
			},
		},
		{
			fields: []Field{{Name: "f1", Type: PrimitiveTypes.Int32, Nullable: true}},
			want: &StructType{
				fields: []Field{{Name: "f1", Type: PrimitiveTypes.Int32, Nullable: true}},
				index:  map[string]int{"f1": 0},
			},
		},
		{
			fields: []Field{
				{Name: "f1", Type: PrimitiveTypes.Int32},
				{Name: "", Type: PrimitiveTypes.Int64},
			},
			want: &StructType{
				fields: []Field{
					{Name: "f1", Type: PrimitiveTypes.Int32},
					{Name: "", Type: PrimitiveTypes.Int64},
				},
				index: map[string]int{"f1": 0, "": 1},
			},
		},
		{
			fields: []Field{
				{Name: "f1", Type: PrimitiveTypes.Int32},
				{Name: "f2", Type: PrimitiveTypes.Int64},
			},
			want: &StructType{
				fields: []Field{
					{Name: "f1", Type: PrimitiveTypes.Int32},
					{Name: "f2", Type: PrimitiveTypes.Int64},
				},
				index: map[string]int{"f1": 0, "f2": 1},
			},
		},
		{
			fields: []Field{
				{Name: "f1", Type: PrimitiveTypes.Int32},
				{Name: "f2", Type: PrimitiveTypes.Int64},
				{Name: "f3", Type: ListOf(PrimitiveTypes.Float64)},
			},
			want: &StructType{
				fields: []Field{
					{Name: "f1", Type: PrimitiveTypes.Int32},
					{Name: "f2", Type: PrimitiveTypes.Int64},
					{Name: "f3", Type: ListOf(PrimitiveTypes.Float64)},
				},
				index: map[string]int{"f1": 0, "f2": 1, "f3": 2},
			},
		},
		{
			fields: []Field{
				{Name: "f1", Type: PrimitiveTypes.Int32},
				{Name: "f2", Type: PrimitiveTypes.Int64},
				{Name: "f3", Type: ListOf(ListOf(PrimitiveTypes.Float64))},
			},
			want: &StructType{
				fields: []Field{
					{Name: "f1", Type: PrimitiveTypes.Int32},
					{Name: "f2", Type: PrimitiveTypes.Int64},
					{Name: "f3", Type: ListOf(ListOf(PrimitiveTypes.Float64))},
				},
				index: map[string]int{"f1": 0, "f2": 1, "f3": 2},
			},
		},
		{
			fields: []Field{
				{Name: "f1", Type: PrimitiveTypes.Int32},
				{Name: "f2", Type: PrimitiveTypes.Int64},
				{Name: "f3", Type: ListOf(ListOf(StructOf(Field{Name: "f1", Type: PrimitiveTypes.Float64})))},
			},
			want: &StructType{
				fields: []Field{
					{Name: "f1", Type: PrimitiveTypes.Int32},
					{Name: "f2", Type: PrimitiveTypes.Int64},
					{Name: "f3", Type: ListOf(ListOf(StructOf(Field{Name: "f1", Type: PrimitiveTypes.Float64})))},
				},
				index: map[string]int{"f1": 0, "f2": 1, "f3": 2},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			got := StructOf(tc.fields...)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got=%#v, want=%#v", got, tc.want)
			}

			if got, want := got.ID(), STRUCT; got != want {
				t.Fatalf("invalid ID. got=%v, want=%v", got, want)
			}

			if got, want := got.Name(), "struct"; got != want {
				t.Fatalf("invalid name. got=%q, want=%q", got, want)
			}

			if got, want := len(got.Fields()), len(tc.fields); got != want {
				t.Fatalf("invalid number of fields. got=%d, want=%d", got, want)
			}

			_, ok := got.FieldByName("not-there")
			if ok {
				t.Fatalf("expected an error")
			}

			if len(tc.fields) > 0 {
				f1, ok := got.FieldByName("f1")
				if !ok {
					t.Fatalf("could not retrieve field 'f1'")
				}
				if f1.HasMetadata() {
					t.Fatalf("field 'f1' should not have metadata")
				}

				for i := range tc.fields {
					f := got.Field(i)
					if f.Name != tc.fields[i].Name {
						t.Fatalf("incorrect named for field[%d]: got=%q, want=%q", i, f.Name, tc.fields[i].Name)
					}
				}
			}
		})
	}

	for _, tc := range []struct {
		fields []Field
	}{
		{
			fields: []Field{
				{Name: "", Type: PrimitiveTypes.Int32},
				{Name: "", Type: PrimitiveTypes.Int32},
			},
		},
		{
			fields: []Field{
				{Name: "x", Type: PrimitiveTypes.Int32},
				{Name: "x", Type: PrimitiveTypes.Int32},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			defer func() {
				e := recover()
				if e == nil {
					t.Fatalf("should have panicked")
				}
			}()
			_ = StructOf(tc.fields...)
		})
	}
}

func TestFieldEqual(t *testing.T) {
	for _, tc := range []struct {
		a, b Field
		want bool
	}{
		{
			a:    Field{},
			b:    Field{},
			want: true,
		},
		{
			a:    Field{Name: "a", Type: PrimitiveTypes.Int32},
			b:    Field{Name: "a", Type: PrimitiveTypes.Int32},
			want: true,
		},
		{
			a:    Field{Name: "a", Type: PrimitiveTypes.Int32, Metadata: MetadataFrom(map[string]string{"k": "v"})},
			b:    Field{Name: "a", Type: PrimitiveTypes.Int32, Metadata: MetadataFrom(map[string]string{"k": "v"})},
			want: true,
		},
		{
			a:    Field{Name: "a", Type: PrimitiveTypes.Int32, Metadata: MetadataFrom(map[string]string{"k": "k"})},
			b:    Field{Name: "a", Type: PrimitiveTypes.Int32, Metadata: MetadataFrom(map[string]string{"k": "v"})},
			want: false,
		},
		{
			a:    Field{Name: "a", Type: PrimitiveTypes.Int32},
			b:    Field{Name: "a", Type: PrimitiveTypes.Int32, Metadata: MetadataFrom(map[string]string{"k": "v"})},
			want: false,
		},
		{
			a:    Field{Name: "a", Type: PrimitiveTypes.Int32},
			b:    Field{Name: "b", Type: PrimitiveTypes.Int32},
			want: false,
		},
		{
			a:    Field{Name: "a", Type: PrimitiveTypes.Int32},
			b:    Field{Name: "a", Type: PrimitiveTypes.Uint32},
			want: false,
		},
	} {
		t.Run("", func(t *testing.T) {
			got := tc.a.Equal(tc.b)
			if got != tc.want {
				t.Fatalf("got=%v, want=%v", got, tc.want)
			}
		})
	}
}

func TestFixedSizeListOf(t *testing.T) {
	for _, tc := range []DataType{
		FixedWidthTypes.Boolean,
		PrimitiveTypes.Int8,
		PrimitiveTypes.Int16,
		PrimitiveTypes.Int32,
		PrimitiveTypes.Int64,
		PrimitiveTypes.Uint8,
		PrimitiveTypes.Uint16,
		PrimitiveTypes.Uint32,
		PrimitiveTypes.Uint64,
		PrimitiveTypes.Float32,
		PrimitiveTypes.Float64,
		ListOf(PrimitiveTypes.Int32),
		FixedSizeListOf(10, PrimitiveTypes.Int32),
		StructOf(),
	} {
		t.Run(tc.Name(), func(t *testing.T) {
			const size = 3
			got := FixedSizeListOf(size, tc)
			want := &FixedSizeListType{elem: Field{Name: "item", Type: tc, Nullable: true}, n: size}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%#v, want=%#v", got, want)
			}

			if got, want := got.Name(), "fixed_size_list"; got != want {
				t.Fatalf("got=%q, want=%q", got, want)
			}

			if got, want := got.ID(), FIXED_SIZE_LIST; got != want {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			if got, want := got.Elem(), tc; got != want {
				t.Fatalf("got=%v, want=%v", got, want)
			}

			if got, want := got.Len(), int32(size); got != want {
				t.Fatalf("got=%v, want=%v", got, want)
			}
		})
	}

	for _, dtype := range []DataType{
		nil,
		// (*Int32Type)(nil), // FIXME(sbinet): should we make sure this is actually caught?
		// (*ListType)(nil), // FIXME(sbinet): should we make sure this is actually caught?
		// (*StructType)(nil), // FIXME(sbinet): should we make sure this is actually caught?
	} {
		t.Run("invalid", func(t *testing.T) {
			defer func() {
				e := recover()
				if e == nil {
					t.Fatalf("test should have panicked but did not")
				}
			}()

			_ = ListOf(dtype)
		})
	}
}

func TestMapOf(t *testing.T) {
	for _, tc := range []struct {
		key, item DataType
		want      DataType
		str       string
	}{
		{
			key:  BinaryTypes.String,
			item: PrimitiveTypes.Uint8,
			want: &MapType{value: ListOf(StructOf(
				Field{Name: "key", Type: BinaryTypes.String},
				Field{Name: "value", Type: PrimitiveTypes.Uint8, Nullable: true},
			))},
			str: "map<utf8, uint8, items_nullable>",
		},
		{
			key:  BinaryTypes.String,
			item: MapOf(PrimitiveTypes.Uint32, FixedWidthTypes.Date32),
			want: &MapType{value: ListOf(StructOf(
				Field{Name: "key", Type: BinaryTypes.String},
				Field{Name: "value", Nullable: true,
					Type: &MapType{value: ListOf(StructOf(
						Field{Name: "key", Type: PrimitiveTypes.Uint32},
						Field{Name: "value", Type: FixedWidthTypes.Date32, Nullable: true},
					))}},
			))},
			str: "map<utf8, map<uint32, date32, items_nullable>, items_nullable>",
		},
	} {
		t.Run("", func(t *testing.T) {
			got := MapOf(tc.key, tc.item)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got=%#v, want=%#v", got, tc.want)
			}

			if got, want := got.ID(), MAP; got != want {
				t.Fatalf("invalid ID. got=%v, want=%v", got, want)
			}

			if got, want := got.Name(), "map"; got != want {
				t.Fatalf("invalid name. got=%q, want=%q", got, want)
			}

			if got, want := got.KeyField().Name, "key"; got != want {
				t.Fatalf("invalid key field name. got=%q, want=%q", got, want)
			}

			if got, want := got.ItemField().Name, "value"; got != want {
				t.Fatalf("invalid item field name. got=%q, want=%q", got, want)
			}

			if got, want := got.KeyType(), tc.key; got != want {
				t.Fatalf("invalid key type. got=%q, want=%q", got, want)
			}

			if got, want := got.ItemType(), tc.item; got != want {
				t.Fatalf("invalid item type. got=%q, want=%q", got, want)
			}

			if got, want := got.ValueType(), StructOf(got.KeyField(), got.ItemField()); !TypeEqual(got, want) {
				t.Fatalf("invalid value type. got=%q, want=%q", got, want)
			}

			if got, want := got.String(), tc.str; got != want {
				t.Fatalf("invalid String() result. got=%q, want=%q", got, want)
			}
		})
	}
}

func TestMapOfWithMetadata(t *testing.T) {
	for _, tc := range []struct {
		key, item                 DataType
		keyMetadata, itemMetadata Metadata
		want                      DataType
		str                       string
	}{
		{
			key:          BinaryTypes.String,
			item:         PrimitiveTypes.Uint8,
			keyMetadata:  NewMetadata([]string{"mk"}, []string{"true"}),
			itemMetadata: NewMetadata([]string{"mi"}, []string{"true"}),
			want: &MapType{value: ListOf(StructOf(
				Field{Name: "key", Type: BinaryTypes.String, Metadata: NewMetadata([]string{"mk"}, []string{"true"})},
				Field{Name: "value", Type: PrimitiveTypes.Uint8, Nullable: true, Metadata: NewMetadata([]string{"mi"}, []string{"true"})},
			))},
			str: "map<utf8, uint8, items_nullable>",
		},
	} {
		t.Run("", func(t *testing.T) {
			got := MapOfWithMetadata(tc.key, NewMetadata([]string{"mk"}, []string{"true"}), tc.item, NewMetadata([]string{"mi"}, []string{"true"}))
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got=%#v, want=%#v", got, tc.want)
			}

			if got, want := got.ID(), MAP; got != want {
				t.Fatalf("invalid ID. got=%v, want=%v", got, want)
			}

			if got, want := got.Name(), "map"; got != want {
				t.Fatalf("invalid name. got=%q, want=%q", got, want)
			}

			if got, want := got.KeyField().Name, "key"; got != want {
				t.Fatalf("invalid key field name. got=%q, want=%q", got, want)
			}

			if got, want := got.ItemField().Name, "value"; got != want {
				t.Fatalf("invalid item field name. got=%q, want=%q", got, want)
			}

			if got, want := got.KeyType(), tc.key; got != want {
				t.Fatalf("invalid key type. got=%q, want=%q", got, want)
			}

			if got, want := got.ItemType(), tc.item; got != want {
				t.Fatalf("invalid item type. got=%q, want=%q", got, want)
			}

			if got, want := got.ValueType(), StructOf(got.KeyField(), got.ItemField()); !TypeEqual(got, want) {
				t.Fatalf("invalid value type. got=%q, want=%q", got, want)
			}

			if got, want := got.String(), tc.str; got != want {
				t.Fatalf("invalid String() result. got=%q, want=%q", got, want)
			}

			if !reflect.DeepEqual(got.ValueType().fields[0].Metadata, tc.keyMetadata) {
				t.Fatalf("invalid key metadata. got=%v, want=%v", got.ValueType().fields[0].Metadata, tc.keyMetadata)
			}
			if !reflect.DeepEqual(got.ValueType().fields[1].Metadata, tc.itemMetadata) {
				t.Fatalf("invalid item metadata. got=%v, want=%v", got.ValueType().fields[1].Metadata, tc.itemMetadata)
			}
		})
	}
}
