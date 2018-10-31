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
)

func TestMetadata(t *testing.T) {
	for _, tc := range []struct {
		md           Metadata
		kvs          map[string]string
		keys, values []string
		err          string
	}{
		{
			md: Metadata{
				keys:   []string{"k1", "k2"},
				values: []string{"v1", "v2"},
			},
			keys:   []string{"k1", "k2"},
			values: []string{"v1", "v2"},
		},
		{
			md: Metadata{},
		},
		{
			md: Metadata{
				keys:   []string{"k1", "k2"},
				values: []string{"v1", "v2"},
			},
			kvs: map[string]string{"k1": "v1", "k2": "v2"},
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
		})
	}
}

func TestSchema(t *testing.T) {
	for _, tc := range []struct {
		fields []Field
		md     *Metadata
		err    error
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
		},
		{
			fields: []Field{
				{Name: "f1", Type: PrimitiveTypes.Int32},
				{Name: "f2", Type: PrimitiveTypes.Int64},
			},
			md: nil,
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
				{Name: "f1", Type: PrimitiveTypes.Int64},
			},
			md:  nil,
			err: fmt.Errorf(`arrow: duplicate field with name "f1"`),
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

			if got, want := len(s.Fields()), len(tc.fields); got != want {
				t.Fatalf("invalid number of fields. got=%d, want=%d", got, want)
			}

			if got, want := s.Field(0), tc.fields[0]; !reflect.DeepEqual(got, want) {
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
				name  string
				ok    bool
				field Field
				i     int
			}{
				{"f1", true, tc.fields[0], 0},
				{"f2", true, tc.fields[1], 1},
				{"N/A", false, Field{}, -1},
			} {
				t.Run(tc.name, func(t *testing.T) {
					got, ok := s.FieldByName(tc.name)
					if ok != tc.ok {
						t.Fatalf("invalid field %q: got=%v, want=%v", tc.name, ok, tc.ok)
					}
					if i := s.FieldIndex(tc.name); i != tc.i {
						t.Fatalf("invalid FieldIndex(%s): got=%v, want=%v", tc.name, i, tc.i)
					}
					if ok := s.HasField(tc.name); ok != tc.ok {
						t.Fatalf("invalid HasField(%s): got=%v, want=%v", tc.name, ok, tc.ok)
					}
					if !reflect.DeepEqual(got, tc.field) {
						t.Fatalf("invalid field: got=%#v, want=%#v", got, tc.field)
					}
				})
			}
		})
	}
}
