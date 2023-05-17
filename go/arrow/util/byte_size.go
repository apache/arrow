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

package util

import (
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
)

func isArrayDataNil(arrayData arrow.ArrayData) bool {
	if arrayData == nil {
		return true
	}
	if v, ok := arrayData.(*array.Data); ok {
		return v == nil
	}
	panic("unknown ArrayData type")
}

func totalArrayDataSize(arrayData arrow.ArrayData, seenBuffers map[*memory.Buffer]struct{}) int64 {
	var sum int64
	var void = struct{}{}
	for _, buf := range arrayData.Buffers() {
		if buf == nil {
			continue
		}
		if _, ok := seenBuffers[buf]; !ok {
			sum += int64(buf.Len())
			seenBuffers[buf] = void
		}
	}
	for _, child := range arrayData.Children() {
		sum += totalArrayDataSize(child, seenBuffers)
	}
	dict := arrayData.Dictionary()
	if !isArrayDataNil(dict) {
		sum += totalArrayDataSize(dict, seenBuffers)
	}
	return sum
}

func totalArraySize(arr arrow.Array, seenBuffers map[*memory.Buffer]struct{}) int64 {
	return totalArrayDataSize(arr.Data(), seenBuffers)
}

func totalRecordSize(record arrow.Record, seenBuffers map[*memory.Buffer]struct{}) int64 {
	var sum int64
	for _, c := range record.Columns() {
		sum += totalArraySize(c, seenBuffers)
	}
	return sum
}

// TotalArraySize returns the sum of the number of bytes in each buffer referenced by the Array.
func TotalArraySize(arr arrow.Array) int64 {
	seenBuffer := make(map[*memory.Buffer]struct{})
	return totalArraySize(arr, seenBuffer)
}

// TotalRecordSize return the sum of bytes in each buffer referenced by the Record.
func TotalRecordSize(record arrow.Record) int64 {
	seenBuffer := make(map[*memory.Buffer]struct{})
	return totalRecordSize(record, seenBuffer)
}
