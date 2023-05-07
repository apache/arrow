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

func TestBinaryType(t *testing.T) {
	var nt *arrow.BinaryType
	if got, want := nt.ID(), arrow.BINARY; got != want {
		t.Fatalf("invalid binary type id. got=%v, want=%v", got, want)
	}

	if got, want := nt.Name(), "binary"; got != want {
		t.Fatalf("invalid binary type name. got=%v, want=%v", got, want)
	}

	if got, want := nt.String(), "binary"; got != want {
		t.Fatalf("invalid binary type stringer. got=%v, want=%v", got, want)
	}
}

func TestStringType(t *testing.T) {
	var nt *arrow.StringType
	if got, want := nt.ID(), arrow.STRING; got != want {
		t.Fatalf("invalid string type id. got=%v, want=%v", got, want)
	}

	if got, want := nt.Name(), "utf8"; got != want {
		t.Fatalf("invalid string type name. got=%v, want=%v", got, want)
	}

	if got, want := nt.String(), "utf8"; got != want {
		t.Fatalf("invalid string type stringer. got=%v, want=%v", got, want)
	}
}

func TestLargeBinaryType(t *testing.T) {
	var nt *arrow.LargeBinaryType
	if got, want := nt.ID(), arrow.LARGE_BINARY; got != want {
		t.Fatalf("invalid binary type id. got=%v, want=%v", got, want)
	}

	if got, want := nt.Name(), "large_binary"; got != want {
		t.Fatalf("invalid binary type name. got=%v, want=%v", got, want)
	}

	if got, want := nt.String(), "large_binary"; got != want {
		t.Fatalf("invalid binary type stringer. got=%v, want=%v", got, want)
	}
}

func TestLargeStringType(t *testing.T) {
	var nt *arrow.LargeStringType
	if got, want := nt.ID(), arrow.LARGE_STRING; got != want {
		t.Fatalf("invalid string type id. got=%v, want=%v", got, want)
	}

	if got, want := nt.Name(), "large_utf8"; got != want {
		t.Fatalf("invalid string type name. got=%v, want=%v", got, want)
	}

	if got, want := nt.String(), "large_utf8"; got != want {
		t.Fatalf("invalid string type stringer. got=%v, want=%v", got, want)
	}
}
