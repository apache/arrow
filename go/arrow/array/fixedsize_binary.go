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

import "github.com/apache/arrow/go/arrow"

// A type which represents an immutable sequence of fixed-length binary strings.
type FixedSizeBinary struct {
	array
	valueOffsets []int32
	valueBytes   []byte
}

// NewFixedSizeBinaryData constructs a new fixed-size binary array from data.
func NewFixedSizeBinaryData(data *Data) *FixedSizeBinary {
	a := &FixedSizeBinary{}
	a.refCount = 1
	a.setData(data)
	return a
}

// Value returns the fixed-size slice at index i. This value should not be mutated.
func (a *FixedSizeBinary) Value(i int) []byte {
	return a.valueBytes[a.valueOffsets[i]:a.valueOffsets[i+1]]
}

func (a *FixedSizeBinary) ValueOffset(i int) int { return int(a.valueOffsets[i]) }
func (a *FixedSizeBinary) ValueLen(i int) int    { return int(a.valueOffsets[i+1] - a.valueOffsets[i]) }
func (a *FixedSizeBinary) ValueOffsets() []int32 { return a.valueOffsets }
func (a *FixedSizeBinary) ValueBytes() []byte    { return a.valueBytes }

func (a *FixedSizeBinary) setData(data *Data) {
	if len(data.buffers) != 3 {
		panic("len(data.buffers) != 3")
	}

	a.array.setData(data)

	if valueBytes := data.buffers[2]; valueBytes != nil {
		a.valueBytes = valueBytes.Bytes()
	}
	if valueOffsets := data.buffers[1]; valueOffsets != nil {
		a.valueOffsets = arrow.Int32Traits.CastFromBytes(valueOffsets.Bytes())
	}
}

var (
	_ Interface = (*FixedSizeBinary)(nil)
)
