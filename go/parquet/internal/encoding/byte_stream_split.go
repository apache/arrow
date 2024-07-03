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

package encoding

import (
	"bytes"

	"github.com/apache/arrow/go/v17/arrow/memory"
)

// encodeByteStreamSplit encodes the raw bytes provided by 'in' into the output buffer 'data' using BYTE_STREAM_SPLIT encoding
func encodeByteStreamSplit(data []byte, in []byte, width int) {
	numElements := len(in) / width
	for stream := 0; stream < width; stream++ {
		for element := 0; element < numElements; element++ {
			encLoc := numElements*stream + element
			decLoc := width*element + stream
			data[encLoc] = in[decLoc]
		}
	}
}

// encodeByteStreamSplitWidth2 implements encodeByteStreamSplit optimized for types stored using 2 bytes
func encodeByteStreamSplitWidth2(data []byte, in []byte) {
	const width = 2
	numElements := len(in) / width
	for element := 0; element < numElements; element++ {
		decLoc := width * element
		data[element] = in[decLoc]
		data[numElements+element] = in[decLoc+1]
	}
}

// encodeByteStreamSplitWidth4 implements encodeByteStreamSplit optimized for types stored using 4 bytes
func encodeByteStreamSplitWidth4(data []byte, in []byte) {
	const width = 4
	numElements := len(in) / width
	for element := 0; element < numElements; element++ {
		decLoc := width * element
		data[element] = in[decLoc]
		data[numElements+element] = in[decLoc+1]
		data[numElements*2+element] = in[decLoc+2]
		data[numElements*3+element] = in[decLoc+3]
	}
}

// encodeByteStreamSplitWidth8 implements encodeByteStreamSplit optimized for types stored using 8 bytes
func encodeByteStreamSplitWidth8(data []byte, in []byte) {
	const width = 8
	numElements := len(in) / width
	for element := 0; element < numElements; element++ {
		decLoc := width * element
		data[element] = in[decLoc]
		data[numElements+element] = in[decLoc+1]
		data[numElements*2+element] = in[decLoc+2]
		data[numElements*3+element] = in[decLoc+3]
		data[numElements*4+element] = in[decLoc+4]
		data[numElements*5+element] = in[decLoc+5]
		data[numElements*6+element] = in[decLoc+6]
		data[numElements*7+element] = in[decLoc+7]
	}
}

// decodeByteStreamSplit decodes the raw bytes provided by 'out' into the output buffer 'data' using BYTE_STREAM_SPLIT encoding
func decodeByteStreamSplit(data []byte, out []byte, width int) {
	numElements := len(data) / width
	for stream := 0; stream < width; stream++ {
		for element := 0; element < numElements; element++ {
			encLoc := numElements*stream + element
			decLoc := width*element + stream
			out[decLoc] = data[encLoc]
		}
	}
}

// decodeByteStreamSplitWidth2 implements decodeByteStreamSplit optimized for types stored using 2 bytes
func decodeByteStreamSplitWidth2(data []byte, out []byte) {
	const width = 2
	numElements := len(data) / width
	for element := 0; element < numElements; element++ {
		decLoc := width * element
		out[decLoc] = data[element]
		out[decLoc+1] = data[numElements+element]
	}
}

// decodeByteStreamSplitWidth4 implements decodeByteStreamSplit optimized for types stored using 4 bytes
func decodeByteStreamSplitWidth4(data []byte, out []byte) {
	const width = 4
	numElements := len(data) / width
	for element := 0; element < numElements; element++ {
		decLoc := width * element
		out[decLoc] = data[element]
		out[decLoc+1] = data[numElements+element]
		out[decLoc+2] = data[numElements*2+element]
		out[decLoc+3] = data[numElements*3+element]
	}
}

// decodeByteStreamSplitWidth8 implements decodeByteStreamSplit optimized for types stored using 8 bytes
func decodeByteStreamSplitWidth8(data []byte, out []byte) {
	const width = 8
	numElements := len(data) / width
	for element := 0; element < numElements; element++ {
		decLoc := width * element
		out[decLoc] = data[element]
		out[decLoc+1] = data[numElements+element]
		out[decLoc+2] = data[numElements*2+element]
		out[decLoc+3] = data[numElements*3+element]
		out[decLoc+4] = data[numElements*4+element]
		out[decLoc+5] = data[numElements*5+element]
		out[decLoc+6] = data[numElements*6+element]
		out[decLoc+7] = data[numElements*7+element]
	}
}

func releaseBufferToPool(pooled *PooledBufferWriter) {
	buf := pooled.buf
	memory.Set(buf.Buf(), 0)
	buf.ResizeNoShrink(0)
	bufferPool.Put(buf)
}

// ByteStreamSplitFloat32Encoder writes the underlying bytes of the Float32
// into interlaced streams as defined by the BYTE_STREAM_SPLIT encoding
type ByteStreamSplitFloat32Encoder struct {
	PlainFloat32Encoder
	flushBuffer *PooledBufferWriter
}

func (enc *ByteStreamSplitFloat32Encoder) FlushValues() (Buffer, error) {
	in, err := enc.PlainFloat32Encoder.FlushValues()
	if err != nil {
		return nil, err
	}

	if enc.flushBuffer == nil {
		enc.flushBuffer = NewPooledBufferWriter(in.Len())
	}

	enc.flushBuffer.buf.Resize(in.Len())
	encodeByteStreamSplitWidth4(enc.flushBuffer.Bytes(), in.Bytes())
	return enc.flushBuffer.Finish(), nil
}

func (enc *ByteStreamSplitFloat32Encoder) Release() {
	enc.PlainFloat32Encoder.Release()
	releaseBufferToPool(enc.flushBuffer)
	enc.flushBuffer = nil
}

// ByteStreamSplitFloat64Encoder writes the underlying bytes of the Float64
// into interlaced streams as defined by the BYTE_STREAM_SPLIT encoding
type ByteStreamSplitFloat64Encoder struct {
	PlainFloat64Encoder
	flushBuffer *PooledBufferWriter
}

func (enc *ByteStreamSplitFloat64Encoder) FlushValues() (Buffer, error) {
	in, err := enc.PlainFloat64Encoder.FlushValues()
	if err != nil {
		return nil, err
	}

	if enc.flushBuffer == nil {
		enc.flushBuffer = NewPooledBufferWriter(in.Len())
	}

	enc.flushBuffer.buf.Resize(in.Len())
	encodeByteStreamSplitWidth8(enc.flushBuffer.Bytes(), in.Bytes())
	return enc.flushBuffer.Finish(), nil
}

func (enc *ByteStreamSplitFloat64Encoder) Release() {
	enc.PlainFloat64Encoder.Release()
	releaseBufferToPool(enc.flushBuffer)
	enc.flushBuffer = nil
}

// ByteStreamSplitInt32Encoder writes the underlying bytes of the Int32
// into interlaced streams as defined by the BYTE_STREAM_SPLIT encoding
type ByteStreamSplitInt32Encoder struct {
	PlainInt32Encoder
	flushBuffer *PooledBufferWriter
}

func (enc *ByteStreamSplitInt32Encoder) FlushValues() (Buffer, error) {
	in, err := enc.PlainInt32Encoder.FlushValues()
	if err != nil {
		return nil, err
	}

	if enc.flushBuffer == nil {
		enc.flushBuffer = NewPooledBufferWriter(in.Len())
	}

	enc.flushBuffer.buf.Resize(in.Len())
	encodeByteStreamSplitWidth4(enc.flushBuffer.Bytes(), in.Bytes())
	return enc.flushBuffer.Finish(), nil
}

func (enc *ByteStreamSplitInt32Encoder) Release() {
	enc.PlainInt32Encoder.Release()
	releaseBufferToPool(enc.flushBuffer)
	enc.flushBuffer = nil
}

// ByteStreamSplitInt64Encoder writes the underlying bytes of the Int64
// into interlaced streams as defined by the BYTE_STREAM_SPLIT encoding
type ByteStreamSplitInt64Encoder struct {
	PlainInt64Encoder
	flushBuffer *PooledBufferWriter
}

func (enc *ByteStreamSplitInt64Encoder) FlushValues() (Buffer, error) {
	in, err := enc.PlainInt64Encoder.FlushValues()
	if err != nil {
		return nil, err
	}

	if enc.flushBuffer == nil {
		enc.flushBuffer = NewPooledBufferWriter(in.Len())
	}

	enc.flushBuffer.buf.Resize(in.Len())
	encodeByteStreamSplitWidth8(enc.flushBuffer.Bytes(), in.Bytes())
	return enc.flushBuffer.Finish(), nil
}

func (enc *ByteStreamSplitInt64Encoder) Release() {
	enc.PlainInt64Encoder.Release()
	releaseBufferToPool(enc.flushBuffer)
	enc.flushBuffer = nil
}

// ByteStreamSplitFloat32Decoder is a decoder for BYTE_STREAM_SPLIT-encoded
// bytes representing Float32 values
type ByteStreamSplitFloat32Decoder struct {
	PlainFloat32Decoder
	pageBuffer bytes.Buffer
}

func (dec *ByteStreamSplitFloat32Decoder) SetData(nvals int, data []byte) error {
	dec.pageBuffer.Grow(len(data))
	buf := dec.pageBuffer.Bytes()[:len(data)]
	decodeByteStreamSplitWidth4(data, buf)
	return dec.PlainFloat32Decoder.SetData(nvals, buf)
}

// ByteStreamSplitFloat64Decoder is a decoder for BYTE_STREAM_SPLIT-encoded
// bytes representing Float64 values
type ByteStreamSplitFloat64Decoder struct {
	PlainFloat64Decoder
	pageBuffer bytes.Buffer
}

func (dec *ByteStreamSplitFloat64Decoder) SetData(nvals int, data []byte) error {
	dec.pageBuffer.Grow(len(data))
	buf := dec.pageBuffer.Bytes()[:len(data)]
	decodeByteStreamSplitWidth8(data, buf)
	return dec.PlainFloat64Decoder.SetData(nvals, buf)
}

// ByteStreamSplitInt32Decoder is a decoder for BYTE_STREAM_SPLIT-encoded
// bytes representing Int32 values
type ByteStreamSplitInt32Decoder struct {
	PlainInt32Decoder
	pageBuffer bytes.Buffer
}

func (dec *ByteStreamSplitInt32Decoder) SetData(nvals int, data []byte) error {
	dec.pageBuffer.Grow(len(data))
	buf := dec.pageBuffer.Bytes()[:len(data)]
	decodeByteStreamSplitWidth4(data, buf)
	return dec.PlainInt32Decoder.SetData(nvals, buf)
}

// ByteStreamSplitInt64Decoder is a decoder for BYTE_STREAM_SPLIT-encoded
// bytes representing Int64 values
type ByteStreamSplitInt64Decoder struct {
	PlainInt64Decoder
	pageBuffer bytes.Buffer
}

func (dec *ByteStreamSplitInt64Decoder) SetData(nvals int, data []byte) error {
	dec.pageBuffer.Grow(len(data))
	buf := dec.pageBuffer.Bytes()[:len(data)]
	decodeByteStreamSplitWidth8(data, buf)
	return dec.PlainInt64Decoder.SetData(nvals, buf)
}
