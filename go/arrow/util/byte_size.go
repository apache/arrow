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
	"fmt"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

func isArrayDataNil(arrayData arrow.ArrayData) bool {
	if arrayData == nil {
		return true
	}
	switch arrayData.(type) {
	case *array.Data:
		v := arrayData.(*array.Data)
		if v == nil {
			return true
		}
	default:
		panic("unknown array data type")
	}
	return false
}

func totalArrayDataSize(arrayData arrow.ArrayData, seenBuffers map[string]struct{}) int64 {
	var sum int64
	for _, buf := range arrayData.Buffers() {
		if buf != nil {
			_, ok := seenBuffers[fmt.Sprintf("%p", buf)]
			if !ok {
				sum += int64(buf.Len())
				seenBuffers[fmt.Sprintf("%p", buf)] = struct{}{}
			}
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


func totalArraySize(arr arrow.Array, seenBuffers map[string]struct{}) int64 {
	return totalArrayDataSize(arr.Data(), seenBuffers)
} 

func totalRecordSize(record arrow.Record, seenBuffers map[string]struct{}) int64 {
	var sum int64
	for _, c := range record.Columns() {
		sum += totalArraySize(c, seenBuffers)
	}
	return sum
} 

func TotalArraySize(arr arrow.Array) int64 {
	seenBuffer := make(map[string]struct{})
	return totalArraySize(arr, seenBuffer)
}

func TotalRecordSize(record arrow.Record) int64 {
	seenBuffer := make(map[string]struct{})
	return totalRecordSize(record, seenBuffer)
}