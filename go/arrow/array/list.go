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

package array

import (
	"github.com/apache/arrow/go/arrow"
)

// List represents an immutable sequence of array values.
type List struct {
	array
	values  Interface
	offsets []int32
}

// NewListData returns a new List array value, from data.
func NewListData(data *Data) *List {
	a := &List{}
	a.refCount = 1
	a.setData(data)
	return a
}

func (a *List) ListValues() Interface { return a.values }

func (a *List) setData(data *Data) {
	a.array.setData(data)
	vals := data.buffers[1]
	if vals != nil {
		a.offsets = arrow.Int32Traits.CastFromBytes(vals.Bytes())
	}
	a.values = MakeFromData(data.childData[0])
}

// Len returns the number of elements in the array.
func (a *List) Len() int { return a.array.Len() }

func (a *List) Offsets() []int32 { return a.offsets }

func (a *List) Release() {
	a.array.Release()
	a.values.Release()
}

var (
	_ Interface = (*List)(nil)
)
