// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package flatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

/// Compressed Sparse format, that is matrix-specific.
type SparseMatrixIndexCSX struct {
	_tab flatbuffers.Table
}

func GetRootAsSparseMatrixIndexCSX(buf []byte, offset flatbuffers.UOffsetT) *SparseMatrixIndexCSX {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &SparseMatrixIndexCSX{}
	x.Init(buf, n+offset)
	return x
}

func FinishSparseMatrixIndexCSXBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsSparseMatrixIndexCSX(buf []byte, offset flatbuffers.UOffsetT) *SparseMatrixIndexCSX {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &SparseMatrixIndexCSX{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedSparseMatrixIndexCSXBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *SparseMatrixIndexCSX) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *SparseMatrixIndexCSX) Table() flatbuffers.Table {
	return rcv._tab
}

/// Which axis, row or column, is compressed
func (rcv *SparseMatrixIndexCSX) CompressedAxis() SparseMatrixCompressedAxis {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return SparseMatrixCompressedAxis(rcv._tab.GetInt16(o + rcv._tab.Pos))
	}
	return 0
}

/// Which axis, row or column, is compressed
func (rcv *SparseMatrixIndexCSX) MutateCompressedAxis(n SparseMatrixCompressedAxis) bool {
	return rcv._tab.MutateInt16Slot(4, int16(n))
}

/// The type of values in indptrBuffer
func (rcv *SparseMatrixIndexCSX) IndptrType(obj *Int) *Int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
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

/// The type of values in indptrBuffer
/// indptrBuffer stores the location and size of indptr array that
/// represents the range of the rows.
/// The i-th row spans from `indptr[i]` to `indptr[i+1]` in the data.
/// The length of this array is 1 + (the number of rows), and the type
/// of index value is long.
///
/// For example, let X be the following 6x4 matrix:
/// ```text
///   X := [[0, 1, 2, 0],
///         [0, 0, 3, 0],
///         [0, 4, 0, 5],
///         [0, 0, 0, 0],
///         [6, 0, 7, 8],
///         [0, 9, 0, 0]].
/// ```
/// The array of non-zero values in X is:
/// ```text
///   values(X) = [1, 2, 3, 4, 5, 6, 7, 8, 9].
/// ```
/// And the indptr of X is:
/// ```text
///   indptr(X) = [0, 2, 3, 5, 5, 8, 10].
/// ```
func (rcv *SparseMatrixIndexCSX) IndptrBuffer(obj *Buffer) *Buffer {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
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

/// indptrBuffer stores the location and size of indptr array that
/// represents the range of the rows.
/// The i-th row spans from `indptr[i]` to `indptr[i+1]` in the data.
/// The length of this array is 1 + (the number of rows), and the type
/// of index value is long.
///
/// For example, let X be the following 6x4 matrix:
/// ```text
///   X := [[0, 1, 2, 0],
///         [0, 0, 3, 0],
///         [0, 4, 0, 5],
///         [0, 0, 0, 0],
///         [6, 0, 7, 8],
///         [0, 9, 0, 0]].
/// ```
/// The array of non-zero values in X is:
/// ```text
///   values(X) = [1, 2, 3, 4, 5, 6, 7, 8, 9].
/// ```
/// And the indptr of X is:
/// ```text
///   indptr(X) = [0, 2, 3, 5, 5, 8, 10].
/// ```
/// The type of values in indicesBuffer
func (rcv *SparseMatrixIndexCSX) IndicesType(obj *Int) *Int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
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

/// The type of values in indicesBuffer
/// indicesBuffer stores the location and size of the array that
/// contains the column indices of the corresponding non-zero values.
/// The type of index value is long.
///
/// For example, the indices of the above X is:
/// ```text
///   indices(X) = [1, 2, 2, 1, 3, 0, 2, 3, 1].
/// ```
/// Note that the indices are sorted in lexicographical order for each row.
func (rcv *SparseMatrixIndexCSX) IndicesBuffer(obj *Buffer) *Buffer {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
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

/// indicesBuffer stores the location and size of the array that
/// contains the column indices of the corresponding non-zero values.
/// The type of index value is long.
///
/// For example, the indices of the above X is:
/// ```text
///   indices(X) = [1, 2, 2, 1, 3, 0, 2, 3, 1].
/// ```
/// Note that the indices are sorted in lexicographical order for each row.
func SparseMatrixIndexCSXStart(builder *flatbuffers.Builder) {
	builder.StartObject(5)
}
func SparseMatrixIndexCSXAddCompressedAxis(builder *flatbuffers.Builder, compressedAxis SparseMatrixCompressedAxis) {
	builder.PrependInt16Slot(0, int16(compressedAxis), 0)
}
func SparseMatrixIndexCSXAddIndptrType(builder *flatbuffers.Builder, indptrType flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(indptrType), 0)
}
func SparseMatrixIndexCSXAddIndptrBuffer(builder *flatbuffers.Builder, indptrBuffer flatbuffers.UOffsetT) {
	builder.PrependStructSlot(2, flatbuffers.UOffsetT(indptrBuffer), 0)
}
func SparseMatrixIndexCSXAddIndicesType(builder *flatbuffers.Builder, indicesType flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(3, flatbuffers.UOffsetT(indicesType), 0)
}
func SparseMatrixIndexCSXAddIndicesBuffer(builder *flatbuffers.Builder, indicesBuffer flatbuffers.UOffsetT) {
	builder.PrependStructSlot(4, flatbuffers.UOffsetT(indicesBuffer), 0)
}
func SparseMatrixIndexCSXEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
