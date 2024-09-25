// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package flatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

/// Compressed Sparse Fiber (CSF) sparse tensor index.
type SparseTensorIndexCSF struct {
	_tab flatbuffers.Table
}

func GetRootAsSparseTensorIndexCSF(buf []byte, offset flatbuffers.UOffsetT) *SparseTensorIndexCSF {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &SparseTensorIndexCSF{}
	x.Init(buf, n+offset)
	return x
}

func FinishSparseTensorIndexCSFBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsSparseTensorIndexCSF(buf []byte, offset flatbuffers.UOffsetT) *SparseTensorIndexCSF {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &SparseTensorIndexCSF{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedSparseTensorIndexCSFBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *SparseTensorIndexCSF) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *SparseTensorIndexCSF) Table() flatbuffers.Table {
	return rcv._tab
}

/// CSF is a generalization of compressed sparse row (CSR) index.
/// See [smith2017knl](http://shaden.io/pub-files/smith2017knl.pdf)
///
/// CSF index recursively compresses each dimension of a tensor into a set
/// of prefix trees. Each path from a root to leaf forms one tensor
/// non-zero index. CSF is implemented with two arrays of buffers and one
/// arrays of integers.
///
/// For example, let X be a 2x3x4x5 tensor and let it have the following
/// 8 non-zero values:
/// ```text
///   X[0, 0, 0, 1] := 1
///   X[0, 0, 0, 2] := 2
///   X[0, 1, 0, 0] := 3
///   X[0, 1, 0, 2] := 4
///   X[0, 1, 1, 0] := 5
///   X[1, 1, 1, 0] := 6
///   X[1, 1, 1, 1] := 7
///   X[1, 1, 1, 2] := 8
/// ```
/// As a prefix tree this would be represented as:
/// ```text
///         0          1
///        / \         |
///       0   1        1
///      /   / \       |
///     0   0   1      1
///    /|  /|   |    /| |
///   1 2 0 2   0   0 1 2
/// ```
/// The type of values in indptrBuffers
func (rcv *SparseTensorIndexCSF) IndptrType(obj *Int) *Int {
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

/// CSF is a generalization of compressed sparse row (CSR) index.
/// See [smith2017knl](http://shaden.io/pub-files/smith2017knl.pdf)
///
/// CSF index recursively compresses each dimension of a tensor into a set
/// of prefix trees. Each path from a root to leaf forms one tensor
/// non-zero index. CSF is implemented with two arrays of buffers and one
/// arrays of integers.
///
/// For example, let X be a 2x3x4x5 tensor and let it have the following
/// 8 non-zero values:
/// ```text
///   X[0, 0, 0, 1] := 1
///   X[0, 0, 0, 2] := 2
///   X[0, 1, 0, 0] := 3
///   X[0, 1, 0, 2] := 4
///   X[0, 1, 1, 0] := 5
///   X[1, 1, 1, 0] := 6
///   X[1, 1, 1, 1] := 7
///   X[1, 1, 1, 2] := 8
/// ```
/// As a prefix tree this would be represented as:
/// ```text
///         0          1
///        / \         |
///       0   1        1
///      /   / \       |
///     0   0   1      1
///    /|  /|   |    /| |
///   1 2 0 2   0   0 1 2
/// ```
/// The type of values in indptrBuffers
/// indptrBuffers stores the sparsity structure.
/// Each two consecutive dimensions in a tensor correspond to a buffer in
/// indptrBuffers. A pair of consecutive values at `indptrBuffers[dim][i]`
/// and `indptrBuffers[dim][i + 1]` signify a range of nodes in
/// `indicesBuffers[dim + 1]` who are children of `indicesBuffers[dim][i]` node.
///
/// For example, the indptrBuffers for the above X is:
/// ```text
///   indptrBuffer(X) = [
///                       [0, 2, 3],
///                       [0, 1, 3, 4],
///                       [0, 2, 4, 5, 8]
///                     ].
/// ```
func (rcv *SparseTensorIndexCSF) IndptrBuffers(obj *Buffer, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 16
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *SparseTensorIndexCSF) IndptrBuffersLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

/// indptrBuffers stores the sparsity structure.
/// Each two consecutive dimensions in a tensor correspond to a buffer in
/// indptrBuffers. A pair of consecutive values at `indptrBuffers[dim][i]`
/// and `indptrBuffers[dim][i + 1]` signify a range of nodes in
/// `indicesBuffers[dim + 1]` who are children of `indicesBuffers[dim][i]` node.
///
/// For example, the indptrBuffers for the above X is:
/// ```text
///   indptrBuffer(X) = [
///                       [0, 2, 3],
///                       [0, 1, 3, 4],
///                       [0, 2, 4, 5, 8]
///                     ].
/// ```
/// The type of values in indicesBuffers
func (rcv *SparseTensorIndexCSF) IndicesType(obj *Int) *Int {
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

/// The type of values in indicesBuffers
/// indicesBuffers stores values of nodes.
/// Each tensor dimension corresponds to a buffer in indicesBuffers.
/// For example, the indicesBuffers for the above X is:
/// ```text
///   indicesBuffer(X) = [
///                        [0, 1],
///                        [0, 1, 1],
///                        [0, 0, 1, 1],
///                        [1, 2, 0, 2, 0, 0, 1, 2]
///                      ].
/// ```
func (rcv *SparseTensorIndexCSF) IndicesBuffers(obj *Buffer, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 16
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *SparseTensorIndexCSF) IndicesBuffersLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

/// indicesBuffers stores values of nodes.
/// Each tensor dimension corresponds to a buffer in indicesBuffers.
/// For example, the indicesBuffers for the above X is:
/// ```text
///   indicesBuffer(X) = [
///                        [0, 1],
///                        [0, 1, 1],
///                        [0, 0, 1, 1],
///                        [1, 2, 0, 2, 0, 0, 1, 2]
///                      ].
/// ```
/// axisOrder stores the sequence in which dimensions were traversed to
/// produce the prefix tree.
/// For example, the axisOrder for the above X is:
/// ```text
///   axisOrder(X) = [0, 1, 2, 3].
/// ```
func (rcv *SparseTensorIndexCSF) AxisOrder(j int) int32 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.GetInt32(a + flatbuffers.UOffsetT(j*4))
	}
	return 0
}

func (rcv *SparseTensorIndexCSF) AxisOrderLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

/// axisOrder stores the sequence in which dimensions were traversed to
/// produce the prefix tree.
/// For example, the axisOrder for the above X is:
/// ```text
///   axisOrder(X) = [0, 1, 2, 3].
/// ```
func (rcv *SparseTensorIndexCSF) MutateAxisOrder(j int, n int32) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.MutateInt32(a+flatbuffers.UOffsetT(j*4), n)
	}
	return false
}

func SparseTensorIndexCSFStart(builder *flatbuffers.Builder) {
	builder.StartObject(5)
}
func SparseTensorIndexCSFAddIndptrType(builder *flatbuffers.Builder, indptrType flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(indptrType), 0)
}
func SparseTensorIndexCSFAddIndptrBuffers(builder *flatbuffers.Builder, indptrBuffers flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(indptrBuffers), 0)
}
func SparseTensorIndexCSFStartIndptrBuffersVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(16, numElems, 8)
}
func SparseTensorIndexCSFAddIndicesType(builder *flatbuffers.Builder, indicesType flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(2, flatbuffers.UOffsetT(indicesType), 0)
}
func SparseTensorIndexCSFAddIndicesBuffers(builder *flatbuffers.Builder, indicesBuffers flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(3, flatbuffers.UOffsetT(indicesBuffers), 0)
}
func SparseTensorIndexCSFStartIndicesBuffersVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(16, numElems, 8)
}
func SparseTensorIndexCSFAddAxisOrder(builder *flatbuffers.Builder, axisOrder flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(4, flatbuffers.UOffsetT(axisOrder), 0)
}
func SparseTensorIndexCSFStartAxisOrderVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func SparseTensorIndexCSFEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
