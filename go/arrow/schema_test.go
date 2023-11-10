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
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v14/arrow/endian"
	"github.com/stretchr/testify/assert"
)

func TestMetadata(t *testing.T) {
	for _, tc := range []struct {
		md           Metadata
		kvs          map[string]string
		keys, values []string
		err          string
		serialize    string
	}{
		{
			md: Metadata{
				keys:   []string{"k1", "k2"},
				values: []string{"v1", "v2"},
			},
			keys:      []string{"k1", "k2"},
			values:    []string{"v1", "v2"},
			serialize: `["k1": "v1", "k2": "v2"]`,
		},
		{
			md:        Metadata{},
			serialize: "[]",
		},
		{
			md: Metadata{
				keys:   []string{"k1", "k2"},
				values: []string{"v1", "v2"},
			},
			kvs:       map[string]string{"k1": "v1", "k2": "v2"},
			serialize: `["k1": "v1", "k2": "v2"]`,
		},
		{
			md:     Metadata{},
			keys:   []string{"k1", "k2", "k3"},
			values: []string{"v1", "v2"},
			err:    "arrow: len mismatch",
		},
	} {
		t.Run("", func(t *testing.T) {
			if tc.err != "" {
				defer func() {
					e := recover()
					if e == nil {
						t.Fatalf("expected a panic")
					}
					if got := e.(string); got != tc.err {
						t.Fatalf("invalid panic. got=%q, want=%q", got, tc.err)
					}
				}()
			}
			var md Metadata
			switch len(tc.kvs) {
			case 0:
				md = NewMetadata(tc.keys, tc.values)
			default:
				md = MetadataFrom(tc.kvs)
			}
			if got, want := md.Len(), len(tc.md.keys); !reflect.DeepEqual(got, want) {
				t.Fatalf("invalid len: got=%v, want=%v", got, want)
			}
			if got, want := md.Keys(), tc.md.keys; !reflect.DeepEqual(got, want) {
				t.Fatalf("invalid keys: got=%v, want=%v", got, want)
			}
			if got, want := md.Values(), tc.md.values; !reflect.DeepEqual(got, want) {
				t.Fatalf("invalid values: got=%v, want=%v", got, want)
			}
			if !reflect.DeepEqual(tc.md, md) {
				t.Fatalf("invalid md: got=%#v, want=%#v", md, tc.md)
			}
			clone := md.clone()
			if !reflect.DeepEqual(clone, md) {
				t.Fatalf("invalid clone: got=%#v, want=%#v", clone, md)
			}

			if got, want := tc.md.String(), tc.serialize; got != want {
				t.Fatalf("invalid stringer: got=%q, want=%q", got, want)
			}
			if len(tc.kvs) != 0 {
				assert.Equal(t, tc.kvs, md.ToMap())
			}
		})
	}

	t.Run("find-key", func(t *testing.T) {
		md := NewMetadata([]string{"k1", "k11"}, []string{"v1", "v11"})

		if got, want := md.FindKey("k1"), 0; got != want {
			t.Fatalf("got=%d, want=%d", got, want)
		}

		gotVal, _ := md.GetValue("k1")
		wantVal := "v1"
		if gotVal != wantVal {
			t.Fatalf("got=%s, want=%s", gotVal, wantVal)
		}

		if got, want := md.FindKey(""), -1; got != want {
			t.Fatalf("got=%d, want=%d", got, want)
		}
		_, gotFound := md.GetValue("")
		if gotFound {
			t.Fatalf("wasn't expecting to find empty key")
		}

		if got, want := md.FindKey("k"), -1; got != want {
			t.Fatalf("got=%d, want=%d", got, want)
		}
		_, gotFound = md.GetValue("k")
		if gotFound {
			t.Fatalf("wasn't expecting to find key: 'k'")
		}

		if got, want := md.FindKey(" "), -1; got != want {
			t.Fatalf("got=%d, want=%d", got, want)
		}

		if got, want := md.FindKey("k11"), 1; got != want {
			t.Fatalf("got=%d, want=%d", got, want)
		}

		if got, want := md.FindKey("k11 "), -1; got != want {
			t.Fatalf("got=%d, want=%d", got, want)
		}
	})
}

func TestSchema(t *testing.T) {
	for _, tc := range []struct {
		fields    []Field
		md        *Metadata
		err       error
		serialize string
		addEndian bool
	}{
		{
			fields: []Field{
				{Name: "f1", Type: PrimitiveTypes.Int32},
				{Name: "f2", Type: PrimitiveTypes.Int64},
			},
			md: func() *Metadata {
				md := MetadataFrom(map[string]string{"k1": "v1", "k2": "v2"})
				return &md
			}(),
			serialize: `schema:
  fields: 2
    - f1: type=int32
    - f2: type=int64
  metadata: ["k1": "v1", "k2": "v2"]`,
		},
		{
			fields: []Field{
				{Name: "f1", Type: PrimitiveTypes.Int32},
				{Name: "f2", Type: PrimitiveTypes.Int64},
			},
			md: nil,
			serialize: `schema:
  fields: 2
    - f1: type=int32
    - f2: type=int64`,
		},
		{
			fields: []Field{
				{Name: "f1", Type: PrimitiveTypes.Int32},
				{Name: "f2", Type: nil},
			},
			md:  nil,
			err: fmt.Errorf("arrow: field with nil DataType"),
		},
		{
			fields: []Field{
				{Name: "f1", Type: PrimitiveTypes.Int32},
				{Name: "f2", Type: PrimitiveTypes.Int64},
				{Name: "dup", Type: PrimitiveTypes.Int32}, // duplicate
				{Name: "dup", Type: PrimitiveTypes.Int64}, // duplicate
			},
			md: nil,
			serialize: `schema:
  fields: 4
    - f1: type=int32
    - f2: type=int64
    - dup: type=int32
    - dup: type=int64`,
		},
		{
			fields: []Field{
				{Name: "f1", Type: PrimitiveTypes.Int32, Nullable: true},
				{Name: "f2", Type: PrimitiveTypes.Uint8},
				{Name: "f3", Type: BinaryTypes.String, Nullable: true},
				{Name: "f4", Type: ListOf(PrimitiveTypes.Int16), Nullable: true},
			},
			md: func() *Metadata {
				md := MetadataFrom(map[string]string{"k1": "v1", "k2": "v2"})
				return &md
			}(),
			addEndian: true, // only print endianness if non-native endian
			serialize: `schema:
  fields: 4
    - f1: type=int32, nullable
    - f2: type=uint8
    - f3: type=utf8, nullable
    - f4: type=list<item: int16, nullable>, nullable
  endianness: ` + endian.NonNativeEndian.String() + `
  metadata: ["k1": "v1", "k2": "v2"]`,
		},
	} {
		t.Run("", func(t *testing.T) {
			if tc.err != nil {
				defer func() {
					e := recover()
					if e == nil {
						t.Fatalf("expected a panic %q", tc.err)
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

			s := NewSchema(tc.fields, tc.md)
			if tc.addEndian {
				s = s.WithEndianness(endian.NonNativeEndian)
			}

			if got, want := len(s.Fields()), len(tc.fields); got != want {
				t.Fatalf("invalid number of fields. got=%d, want=%d", got, want)
			}

			if got, want := s.Field(0), tc.fields[0]; !got.Equal(want) {
				t.Fatalf("invalid field: got=%#v, want=%#v", got, want)
			}

			fields := s.Fields()
			fields[0].Name = "other"
			// check that the fields are copied and not shared
			if got, want := s.Field(0), tc.fields[0]; !got.Equal(want) {
				t.Fatalf("invalid field: got=%#v, want=%#v", got, want)
			}

			if got, want := s.HasMetadata(), tc.md != nil; got != want {
				t.Fatalf("invalid metadata: got=%v, want=%v", got, want)
			}

			if tc.md != nil {
				if got, want := s.Metadata(), *tc.md; !reflect.DeepEqual(got, want) {
					t.Fatalf("invalid metadata: got=%#v, want=%#v", got, want)
				}
			}

			for _, tc := range []struct {
				name   string
				ok     bool
				fields []Field
				i      []int
			}{
				{"f1", true, []Field{tc.fields[0]}, []int{0}},
				{"f2", true, []Field{tc.fields[1]}, []int{1}},
				{"N/A", false, nil, nil},
			} {
				t.Run(tc.name, func(t *testing.T) {
					got, ok := s.FieldsByName(tc.name)
					if ok != tc.ok {
						t.Fatalf("invalid field %q: got=%v, want=%v", tc.name, ok, tc.ok)
					}
					if i := s.FieldIndices(tc.name); !reflect.DeepEqual(i, tc.i) {
						t.Fatalf("invalid FieldIndices(%s): got=%v, want=%v\nfields: %v", tc.name, i, tc.i, s.fields)
					}
					if ok := s.HasField(tc.name); ok != tc.ok {
						t.Fatalf("invalid HasField(%s): got=%v, want=%v", tc.name, ok, tc.ok)
					}
					for i, field := range got {
						if !field.Equal(tc.fields[i]) {
							t.Fatalf("invalid field[%d]: got=%#v, want=%#v", i, field, tc.fields[i])
						}
					}
				})
			}

			if s.HasField("dup") {
				got := s.FieldIndices("dup")
				want := []int{2, 3}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("invalid duplicate fields: got=%v, want=%v", got, want)
				}
			}

			if got, want := s.String(), tc.serialize; got != want {
				t.Fatalf("invalid stringer: got=%q, want=%q", got, want)
			}
		})
	}
}

func TestSchemaAddField(t *testing.T) {
	s := NewSchema([]Field{
		{Name: "f1", Type: PrimitiveTypes.Int32},
		{Name: "f2", Type: PrimitiveTypes.Int64},
	}, nil)

	_, err := s.AddField(3, Field{Name: "f3", Type: PrimitiveTypes.Int32})
	if err == nil {
		t.Fatalf("expected an error")
	}

	s, err = s.AddField(2, Field{Name: "f3", Type: PrimitiveTypes.Int32})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := len(s.Fields()), 3; got != want {
		t.Fatalf("invalid number of fields. got=%d, want=%d", got, want)
	}
	got, want := s.Field(2), Field{Name: "f3", Type: PrimitiveTypes.Int32}
	if !got.Equal(want) {
		t.Fatalf("invalid field: got=%#v, want=%#v", got, want)
	}
}

func TestSchemaEqual(t *testing.T) {
	fields := []Field{
		{Name: "f1", Type: PrimitiveTypes.Int32},
		{Name: "f2", Type: PrimitiveTypes.Int64},
	}
	md := func() *Metadata {
		md := MetadataFrom(map[string]string{"k1": "v1", "k2": "v2"})
		return &md
	}()

	for _, tc := range []struct {
		a, b *Schema
		want bool
	}{
		{
			a:    nil,
			b:    nil,
			want: true,
		},
		{
			a:    nil,
			b:    NewSchema(nil, nil),
			want: false,
		},
		{
			a:    NewSchema(nil, nil),
			b:    nil,
			want: false,
		},
		{
			a:    NewSchema(nil, nil),
			b:    NewSchema(nil, nil),
			want: true,
		},
		{
			a:    NewSchema(fields, nil),
			b:    NewSchema(fields, nil),
			want: true,
		},
		{
			a:    NewSchema(fields, md),
			b:    NewSchema(fields, nil),
			want: true,
		},
		{
			a:    NewSchema(fields, md),
			b:    NewSchema(fields, md),
			want: true,
		},
		{
			a:    NewSchema(fields[:1], md),
			b:    NewSchema(fields, md),
			want: false,
		},
		{
			a: NewSchema(fields, md),
			b: NewSchema([]Field{
				{Name: "f1", Type: PrimitiveTypes.Int32},
				{Name: "f2", Type: PrimitiveTypes.Int32},
			}, md),
			want: false,
		},
		{
			a: NewSchema(fields, md),
			b: NewSchema([]Field{
				{Name: "f1", Type: PrimitiveTypes.Int32},
				{Name: "fx", Type: PrimitiveTypes.Int64},
			}, md),
			want: false,
		},
		{
			a:    NewSchemaWithEndian(fields, nil, endian.LittleEndian),
			b:    NewSchemaWithEndian(fields, nil, endian.LittleEndian),
			want: true,
		},
		{
			a:    NewSchemaWithEndian(fields, nil, endian.LittleEndian),
			b:    NewSchemaWithEndian(fields, nil, endian.BigEndian),
			want: false,
		},
		{
			a:    NewSchemaWithEndian(fields, nil, endian.LittleEndian),
			b:    NewSchema(fields, nil),
			want: !endian.IsBigEndian,
		},
		{
			a:    NewSchemaWithEndian(fields, nil, endian.BigEndian),
			b:    NewSchema(fields, nil),
			want: endian.IsBigEndian,
		},
	} {
		t.Run("", func(t *testing.T) {
			if !tc.a.Equal(tc.a) {
				t.Fatalf("a != a")
			}
			if !tc.b.Equal(tc.b) {
				t.Fatalf("b != b")
			}
			ab := tc.a.Equal(tc.b)
			if ab != tc.want {
				t.Fatalf("got=%v, want=%v", ab, tc.want)
			}

			ba := tc.b.Equal(tc.a)
			if ab != ba {
				t.Fatalf("ab != ba")
			}

			if (tc.a.Fingerprint() == tc.b.Fingerprint()) != tc.want {
				t.Fatalf("fingerprint: got=%v;%v, wanted=%v", tc.a.Fingerprint(), tc.b.Fingerprint(), tc.want)
			}
		})
	}
}

func TestSchemaNumFields(t *testing.T) {
	s := NewSchema([]Field{
		{Name: "f1", Type: PrimitiveTypes.Int32},
		{Name: "f2", Type: PrimitiveTypes.Int64},
	}, nil)

	assert.Equal(t, 2, s.NumFields())

	var err error
	s, err = s.AddField(2, Field{Name: "f3", Type: PrimitiveTypes.Int32})
	assert.NoError(t, err)

	assert.Equal(t, 3, s.NumFields())
	assert.Equal(t, s.NumFields(), len(s.Fields()))
}
