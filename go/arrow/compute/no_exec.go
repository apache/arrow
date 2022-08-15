// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// this file is used to provide dummy implementations for exec
// functions that are called elsewhere in the compute package by
// the expression handlers so that the logic can stay where it should
// belong.

package compute

import (
	"context"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/memory"
)

// dummy function which always returns false when not loading the C++ lib
func isFuncScalar(funcName string) bool {
	return false
}

type boundRef uintptr

func (boundRef) release() {}

// when compiled without the c++ library (the build tags control whether it looks for it)
// then we do not have pure go implementation of the expression binding currently.
func bindExprSchema(context.Context, memory.Allocator, Expression, *arrow.Schema) (boundRef, ValueDescr, int, Expression, error) {
	panic("arrow/compute: bind expression not implemented")
}
