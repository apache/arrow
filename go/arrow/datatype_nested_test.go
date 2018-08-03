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
	} {
		t.Run(tc.Name(), func(t *testing.T) {
			got := ListOf(tc)
			want := &ListType{elem: tc}
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
}
