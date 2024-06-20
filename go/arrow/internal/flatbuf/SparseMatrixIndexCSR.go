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

// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package flatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

// / Compressed Sparse Row format, that is matrix-specific.
type SparseMatrixIndexCSR struct {
	_tab flatbuffers.Table
}

func GetRootAsSparseMatrixIndexCSR(buf []byte, offset flatbuffers.UOffsetT) *SparseMatrixIndexCSR {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &SparseMatrixIndexCSR{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *SparseMatrixIndexCSR) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *SparseMatrixIndexCSR) Table() flatbuffers.Table {
	return rcv._tab
}

// / The type of values in indptrBuffer
func (rcv *SparseMatrixIndexCSR) IndptrType(obj *Int) *Int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(Int)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

// / The type of values in indptrBuffer
// / indptrBuffer stores the location and size of indptr array that
// / represents the range of the rows.
// / The i-th row spans from indptr[i] to indptr[i+1] in the data.
// / The length of this array is 1 + (the number of rows), and the type
// / of index value is long.
// /
// / For example, let X be the following 6x4 matrix:
// /
// /   X := [[0, 1, 2, 0],
// /         [0, 0, 3, 0],
// /         [0, 4, 0, 5],
// /         [0, 0, 0, 0],
// /         [6, 0, 7, 8],
// /         [0, 9, 0, 0]].
// /
// / The array of non-zero values in X is:
// /
// /   values(X) = [1, 2, 3, 4, 5, 6, 7, 8, 9].
// /
// / And the indptr of X is:
// /
// /   indptr(X) = [0, 2, 3, 5, 5, 8, 10].
func (rcv *SparseMatrixIndexCSR) IndptrBuffer(obj *Buffer) *Buffer {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		x := o + rcv._tab.Pos
		if obj == nil {
			obj = new(Buffer)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

// / indptrBuffer stores the location and size of indptr array that
// / represents the range of the rows.
// / The i-th row spans from indptr[i] to indptr[i+1] in the data.
// / The length of this array is 1 + (the number of rows), and the type
// / of index value is long.
// /
// / For example, let X be the following 6x4 matrix:
// /
// /   X := [[0, 1, 2, 0],
// /         [0, 0, 3, 0],
// /         [0, 4, 0, 5],
// /         [0, 0, 0, 0],
// /         [6, 0, 7, 8],
// /         [0, 9, 0, 0]].
// /
// / The array of non-zero values in X is:
// /
// /   values(X) = [1, 2, 3, 4, 5, 6, 7, 8, 9].
// /
// / And the indptr of X is:
// /
// /   indptr(X) = [0, 2, 3, 5, 5, 8, 10].
// / The type of values in indicesBuffer
func (rcv *SparseMatrixIndexCSR) IndicesType(obj *Int) *Int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(Int)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

// / The type of values in indicesBuffer
// / indicesBuffer stores the location and size of the array that
// / contains the column indices of the corresponding non-zero values.
// / The type of index value is long.
// /
// / For example, the indices of the above X is:
// /
// /   indices(X) = [1, 2, 2, 1, 3, 0, 2, 3, 1].
// /
// / Note that the indices are sorted in lexicographical order for each row.
func (rcv *SparseMatrixIndexCSR) IndicesBuffer(obj *Buffer) *Buffer {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		x := o + rcv._tab.Pos
		if obj == nil {
			obj = new(Buffer)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

// / indicesBuffer stores the location and size of the array that
// / contains the column indices of the corresponding non-zero values.
// / The type of index value is long.
// /
// / For example, the indices of the above X is:
// /
// /   indices(X) = [1, 2, 2, 1, 3, 0, 2, 3, 1].
// /
// / Note that the indices are sorted in lexicographical order for each row.
func SparseMatrixIndexCSRStart(builder *flatbuffers.Builder) {
	builder.StartObject(4)
}
func SparseMatrixIndexCSRAddIndptrType(builder *flatbuffers.Builder, indptrType flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(indptrType), 0)
}
func SparseMatrixIndexCSRAddIndptrBuffer(builder *flatbuffers.Builder, indptrBuffer flatbuffers.UOffsetT) {
	builder.PrependStructSlot(1, flatbuffers.UOffsetT(indptrBuffer), 0)
}
func SparseMatrixIndexCSRAddIndicesType(builder *flatbuffers.Builder, indicesType flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(2, flatbuffers.UOffsetT(indicesType), 0)
}
func SparseMatrixIndexCSRAddIndicesBuffer(builder *flatbuffers.Builder, indicesBuffer flatbuffers.UOffsetT) {
	builder.PrependStructSlot(3, flatbuffers.UOffsetT(indicesBuffer), 0)
}
func SparseMatrixIndexCSREnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
