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

package arrow_test

import (
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
)

func TestNullType(t *testing.T) {
	var nt *arrow.NullType
	if got, want := nt.ID(), arrow.NULL; got != want {
		t.Fatalf("invalid null type id. got=%v, want=%v", got, want)
	}

	if got, want := nt.Name(), "null"; got != want {
		t.Fatalf("invalid null type name. got=%q, want=%q", got, want)
	}

	if got, want := nt.String(), "null"; got != want {
		t.Fatalf("invalid null type stringer. got=%q, want=%q", got, want)
	}
}
