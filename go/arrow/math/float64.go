// Code generated by type.go.tmpl. DO NOT EDIT.

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

package math

import (
	"github.com/apache/arrow/go/arrow/array"
)

type Float64Funcs struct {
	sum func(a *array.Float64) float64
}

var (
	Float64 Float64Funcs
)

// Sum returns the summation of all elements in a.
func (f Float64Funcs) Sum(a *array.Float64) float64 {
	return f.sum(a)
}

func sum_float64_go(a *array.Float64) float64 {
	acc := float64(0)
	for _, v := range a.Float64Values() {
		acc += v
	}
	return acc
}
