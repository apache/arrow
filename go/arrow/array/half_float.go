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
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"strings"
)

// A type which represents an immutable sequence of Float16 values.
type HalfFloat struct {
	array
	values []arrow.Float16
}

func NewHalfFloatData(data *Data) *HalfFloat {
	a := &HalfFloat{}
	a.refCount = 1
	a.setData(data)
	return a
}

func (a *HalfFloat) Value(i int) float32 { return a.values[i].Float32() }
func (a *HalfFloat) Values() []float32 {
	values := make([]float32, len(a.values))
	for i, v := range a.values {
		values[i] = v.Float32()
	}
	return values
}

func (a *HalfFloat) String() string {
	o := new(strings.Builder)
	o.WriteString("[")
	for i, v := range a.values {
		if i > 0 {
			fmt.Fprintf(o, " ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString("(null)")
		default:
			fmt.Fprintf(o, "%v", v.Float32())
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *HalfFloat) setData(data *Data) {
	a.array.setData(data)
	vals := data.buffers[1]
	if vals != nil {
		a.values = arrow.Float16Traits.CastFromBytes(vals.Bytes())
		beg := a.array.data.offset
		end := beg + a.array.data.length
		a.values = a.values[beg:end]
	}
}

var (
	_ Interface = (*HalfFloat)(nil)
)
