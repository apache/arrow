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

package ipc_test

import (
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow/go/arrow/array"
)

func cmpRecs(r1, r2 array.Record) bool {
	// FIXME(sbinet): impl+use arrow.Record.Equal ?

	if !r1.Schema().Equal(r2.Schema()) {
		return false
	}
	if r1.NumCols() != r2.NumCols() {
		return false
	}
	if r1.NumRows() != r2.NumRows() {
		return false
	}

	var (
		txt1 = new(strings.Builder)
		txt2 = new(strings.Builder)
	)

	printRec(txt1, r1)
	printRec(txt2, r2)

	return txt1.String() == txt2.String()
}

func printRec(w io.Writer, rec array.Record) {
	for i, col := range rec.Columns() {
		fmt.Fprintf(w, "  col[%d] %q: %v\n", i, rec.ColumnName(i), col)
	}
}
