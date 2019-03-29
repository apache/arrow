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
	"testing"
)

func TestTypeEquals(t *testing.T) {
	tests := []struct {
		left, right   DataType
		want          bool
		checkMetadata bool
	}{
		{
			nil, nil, false, false,
		},
		{
			nil, PrimitiveTypes.Uint8, false, false,
		},
		{
			PrimitiveTypes.Float32, nil, false, false,
		},
		{
			PrimitiveTypes.Float64, PrimitiveTypes.Int32, false, false,
		},
		{
			Null, Null, true, false,
		},
		{
			&Time32Type{Unit: Second}, &Time32Type{Unit: Second}, true, false,
		},
		{
			&Time32Type{Unit: Millisecond}, &Time32Type{Unit: Second}, false, false,
		},
		{
			&Time64Type{Unit: Nanosecond}, &Time64Type{Unit: Nanosecond}, true, false,
		},
		{
			&Time64Type{Unit: Nanosecond}, &Time64Type{Unit: Microsecond}, false, false,
		},
		{
			&TimestampType{Unit: Second, TimeZone: "UTC"}, &TimestampType{Unit: Second, TimeZone: "UTC"}, true, false,
		},
		{
			&TimestampType{Unit: Microsecond, TimeZone: "UTC"}, &TimestampType{Unit: Millisecond, TimeZone: "UTC"}, false, false,
		},
		{
			&TimestampType{Unit: Second, TimeZone: "UTC"}, &TimestampType{Unit: Second, TimeZone: "CET"}, false, false,
		},
		{
			&TimestampType{Unit: Second, TimeZone: "UTC"}, &TimestampType{Unit: Nanosecond, TimeZone: "CET"}, false, false,
		},
		{
			&ListType{PrimitiveTypes.Uint64}, &ListType{PrimitiveTypes.Uint64}, true, false,
		},
		{
			&ListType{PrimitiveTypes.Uint64}, &ListType{PrimitiveTypes.Uint32}, false, false,
		},
		{
			&ListType{&Time32Type{Unit: Millisecond}}, &ListType{&Time32Type{Unit: Millisecond}}, true, false,
		},
		{
			&ListType{&Time32Type{Unit: Millisecond}}, &ListType{&Time32Type{Unit: Second}}, false, false,
		},
		{
			&ListType{&ListType{PrimitiveTypes.Uint16}}, &ListType{&ListType{PrimitiveTypes.Uint16}}, true, false,
		},
		{
			&ListType{&ListType{PrimitiveTypes.Uint16}}, &ListType{&ListType{PrimitiveTypes.Uint8}}, false, false,
		},
		{
			&ListType{&ListType{&ListType{PrimitiveTypes.Uint16}}}, &ListType{&ListType{PrimitiveTypes.Uint8}}, false, false,
		},
		{
			&StructType{
				fields: []Field{
					Field{Name: "f1", Type: PrimitiveTypes.Uint16, Nullable: true},
				},
				index: map[string]int{"f1": 0},
			},
			&StructType{
				fields: []Field{
					Field{Name: "f1", Type: PrimitiveTypes.Uint32, Nullable: true},
				},
				index: map[string]int{"f1": 0},
			},
			false, true,
		},
		{
			&StructType{
				fields: []Field{
					Field{Name: "f1", Type: PrimitiveTypes.Uint32, Nullable: false},
				},
				index: map[string]int{"f1": 0},
			},
			&StructType{
				fields: []Field{
					Field{Name: "f1", Type: PrimitiveTypes.Uint32, Nullable: true},
				},
				index: map[string]int{"f1": 0},
			},
			false, false,
		},
		{
			&StructType{
				fields: []Field{
					Field{Name: "f0", Type: PrimitiveTypes.Uint32, Nullable: true},
				},
				index: map[string]int{"f0": 0},
			},
			&StructType{
				fields: []Field{
					Field{Name: "f1", Type: PrimitiveTypes.Uint32, Nullable: true},
				},
				index: map[string]int{"f1": 0},
			},
			false, false,
		},
		{
			&StructType{
				fields: []Field{
					Field{Name: "f1", Type: PrimitiveTypes.Uint32, Nullable: true},
				},
				index: map[string]int{"f1": 0},
			},
			&StructType{
				fields: []Field{
					Field{Name: "f1", Type: PrimitiveTypes.Uint32, Nullable: true},
					Field{Name: "f2", Type: PrimitiveTypes.Uint32, Nullable: true},
				},
				index: map[string]int{"f1": 0, "f2": 0},
			},
			false, true,
		},
		{
			&StructType{
				fields: []Field{
					Field{Name: "f1", Type: PrimitiveTypes.Uint16, Nullable: true},
					Field{Name: "f2", Type: PrimitiveTypes.Float32, Nullable: false},
				},
				index: map[string]int{"f1": 0, "f2": 1},
			},
			&StructType{
				fields: []Field{
					Field{Name: "f1", Type: PrimitiveTypes.Uint16, Nullable: true},
					Field{Name: "f2", Type: PrimitiveTypes.Float32, Nullable: false},
				},
				index: map[string]int{"f1": 0, "f2": 1},
			},
			true, false,
		},
		{
			&StructType{
				fields: []Field{
					Field{Name: "f1", Type: PrimitiveTypes.Uint16, Nullable: true},
					Field{Name: "f2", Type: PrimitiveTypes.Float32, Nullable: false},
				},
				index: map[string]int{"f1": 0, "f2": 1},
			},
			&StructType{
				fields: []Field{
					Field{Name: "f1", Type: PrimitiveTypes.Uint16, Nullable: true},
					Field{Name: "f2", Type: PrimitiveTypes.Float32, Nullable: false},
				},
				index: map[string]int{"f1": 0, "f2": 1},
			},
			true, false,
		},
		{
			&StructType{
				fields: []Field{
					Field{Name: "f1", Type: PrimitiveTypes.Uint16, Nullable: true},
					Field{Name: "f2", Type: PrimitiveTypes.Float32, Nullable: false},
				},
				index: map[string]int{"f1": 0, "f2": 1},
				meta:  MetadataFrom(map[string]string{"k1": "v1"}),
			},
			&StructType{
				fields: []Field{
					Field{Name: "f1", Type: PrimitiveTypes.Uint16, Nullable: true},
					Field{Name: "f2", Type: PrimitiveTypes.Float32, Nullable: false},
				},
				index: map[string]int{"f1": 0, "f2": 1},
				meta:  MetadataFrom(map[string]string{"k1": "v1"}),
			},
			true, true,
		},
		{
			&StructType{
				fields: []Field{
					Field{Name: "f1", Type: PrimitiveTypes.Uint32, Nullable: true},
				},
				index: map[string]int{"f1": 0},
				meta:  MetadataFrom(map[string]string{"k1": "v1"}),
			},
			&StructType{
				fields: []Field{
					Field{Name: "f1", Type: PrimitiveTypes.Uint32, Nullable: true},
				},
				index: map[string]int{"f1": 0},
				meta:  MetadataFrom(map[string]string{"k1": "v2"}),
			},
			true, false,
		},
		{
			&StructType{
				fields: []Field{
					Field{Name: "f1", Type: PrimitiveTypes.Uint16, Nullable: true, Metadata: MetadataFrom(map[string]string{"k1": "v1"})},
					Field{Name: "f2", Type: PrimitiveTypes.Float32, Nullable: false},
				},
				index: map[string]int{"f1": 0, "f2": 1},
			},
			&StructType{
				fields: []Field{
					Field{Name: "f1", Type: PrimitiveTypes.Uint16, Nullable: true, Metadata: MetadataFrom(map[string]string{"k1": "v2"})},
					Field{Name: "f2", Type: PrimitiveTypes.Float32, Nullable: false},
				},
				index: map[string]int{"f1": 0, "f2": 1},
			},
			false, true,
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			var got bool
			if test.checkMetadata {
				got = TypeEquals(test.left, test.right, CheckMetadata())
			} else {
				got = TypeEquals(test.left, test.right)
			}
			if got != test.want {
				t.Fatalf("TypeEquals(%v, %v, %v): got=%v, want=%v", test.left, test.right, test.checkMetadata, got, test.want)
			}
		})
	}
}
