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

// ListType describes a nested type in which each array slot contains
// a variable-size sequence of values, all having the same relative type.
type ListType struct {
	elem DataType // DataType of the list's elements
}

// ListOf returns the list type with element type t.
// For example, if t represents int32, ListOf(t) represents []int32.
//
// ListOf panics if t is nil or invalid.
func ListOf(t DataType) *ListType {
	if t == nil {
		panic("arrow: nil DataType")
	}
	return &ListType{elem: t}
}

func (*ListType) ID() Type     { return LIST }
func (*ListType) Name() string { return "list" }

// Elem returns the ListType's element type.
func (t *ListType) Elem() DataType { return t.elem }

var (
	_ DataType = (*ListType)(nil)
)
