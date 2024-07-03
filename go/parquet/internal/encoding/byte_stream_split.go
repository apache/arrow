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
	"math"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/internal/utils"
	"github.com/apache/arrow/go/v17/parquet"
	"golang.org/x/xerrors"
)

type NumericByteStreamSplitType interface {
	float32 | float64 | int32 | int64
}

type ByteStreamSplitType interface {
	NumericByteStreamSplitType | parquet.FixedLenByteArray
}

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
	width := 2
	numElements := len(in) / width
	for element := 0; element < numElements; element++ {
		decLoc := width * element
		data[element] = in[decLoc]
		data[numElements+element] = in[decLoc+1]
	}
}

// encodeByteStreamSplitWidth4 implements encodeByteStreamSplit optimized for types stored using 4 bytes
func encodeByteStreamSplitWidth4(data []byte, in []byte) {
	width := 4
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
	width := 8
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

// decodeByteStreamSplit is a generic implementation of the BYTE_STREAM_SPLIT decoder for fixed-width numeric types.
func decodeByteStreamSplitNumeric[T NumericByteStreamSplitType](out []T, dec TypedDecoder, data []byte) (int, error) {
	max := utils.Min(len(out), dec.ValuesLeft())
	typeLen := dec.Type().ByteSize()
	numBytesNeeded := max * typeLen
	if numBytesNeeded > len(data) || numBytesNeeded > math.MaxInt32 {
		return 0, xerrors.New("parquet: eof exception")
	}

	outBytes := arrow.GetBytes(out)
	switch typeLen {
	case 4:
		decodeByteStreamSplitWidth4(data, outBytes, max)
	case 8:
		decodeByteStreamSplitWidth8(data, outBytes, max)
	default:
		decodeByteStreamSplit(data, outBytes, max, typeLen)
	}

	dec.SetData(dec.ValuesLeft()-max, data[numBytesNeeded:])

	return max, nil
}

// decodeByteStreamSplit decodes the raw bytes provided by 'out' into the output buffer 'data' using BYTE_STREAM_SPLIT encoding
func decodeByteStreamSplit(data []byte, out []byte, numElements, width int) {
	for stream := 0; stream < width; stream++ {
		for element := 0; element < numElements; element++ {
			encLoc := numElements*stream + element
			decLoc := width*element + stream
			out[decLoc] = data[encLoc]
		}
	}
}

// decodeByteStreamSplitWidth4 implements decodeByteStreamSplit optimized for types stored using 4 bytes
func decodeByteStreamSplitWidth4(data []byte, out []byte, numElements int) {
	width := 4
	for element := 0; element < numElements; element++ {
		decLoc := width * element
		out[decLoc] = data[element]
		out[decLoc+1] = data[numElements+element]
		out[decLoc+2] = data[numElements*2+element]
		out[decLoc+3] = data[numElements*3+element]
	}
}

// decodeByteStreamSplitWidth8 implements decodeByteStreamSplit optimized for types stored using 8 bytes
func decodeByteStreamSplitWidth8(data []byte, out []byte, numElements int) {
	width := 8
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

// decodeByteStreamSplitSpaced decodes BYTE_STREAM_SPLIT-encoded data with space for nulls, calling to the provided decodeFn to decode runs of non-null values.
func decodeByteStreamSplitSpaced[T ByteStreamSplitType](out []T, nullCount int, validBits []byte, validBitsOffset int64, decodeFn func([]T) (int, error)) (int, error) {
	toRead := len(out) - nullCount
	valuesRead, err := decodeFn(out[:toRead])
	if err != nil {
		return valuesRead, err
	}
	if valuesRead != toRead {
		return valuesRead, xerrors.New("parquet: number of values / definitions levels read did not match")
	}

	return spacedExpand(out, nullCount, validBits, validBitsOffset), nil
}

func flushByteStreamSplit(enc TypedEncoder) (Buffer, *PooledBufferWriter, error) {
	in, err := enc.FlushValues()
	if err != nil {
		return nil, nil, err
	}

	out := NewPooledBufferWriter(in.Len())
	out.buf.ResizeNoShrink(in.Len())

	return in, out, nil
}

// ByteStreamSplitFloat32Encoder writes the underlying bytes of the Float32
// into interlaced streams as defined by the BYTE_STREAM_SPLIT encoding
type ByteStreamSplitFloat32Encoder struct {
	PlainFloat32Encoder
}

func (enc *ByteStreamSplitFloat32Encoder) FlushValues() (Buffer, error) {
	in, out, err := flushByteStreamSplit(&enc.PlainFloat32Encoder)
	if err != nil {
		return nil, err
	}
	encodeByteStreamSplitWidth4(out.Bytes(), in.Bytes())
	return out.Finish(), nil
}

// ByteStreamSplitFloat64Encoder writes the underlying bytes of the Float64
// into interlaced streams as defined by the BYTE_STREAM_SPLIT encoding
type ByteStreamSplitFloat64Encoder struct {
	PlainFloat64Encoder
}

func (enc *ByteStreamSplitFloat64Encoder) FlushValues() (Buffer, error) {
	in, out, err := flushByteStreamSplit(&enc.PlainFloat64Encoder)
	if err != nil {
		return nil, err
	}
	encodeByteStreamSplitWidth8(out.Bytes(), in.Bytes())
	return out.Finish(), nil
}

// ByteStreamSplitInt32Encoder writes the underlying bytes of the Int32
// into interlaced streams as defined by the BYTE_STREAM_SPLIT encoding
type ByteStreamSplitInt32Encoder struct {
	PlainInt32Encoder
}

func (enc *ByteStreamSplitInt32Encoder) FlushValues() (Buffer, error) {
	in, out, err := flushByteStreamSplit(&enc.PlainInt32Encoder)
	if err != nil {
		return nil, err
	}
	encodeByteStreamSplitWidth4(out.Bytes(), in.Bytes())
	return out.Finish(), nil
}

// ByteStreamSplitInt64Encoder writes the underlying bytes of the Int64
// into interlaced streams as defined by the BYTE_STREAM_SPLIT encoding
type ByteStreamSplitInt64Encoder struct {
	PlainInt64Encoder
}

func (enc *ByteStreamSplitInt64Encoder) FlushValues() (Buffer, error) {
	in, out, err := flushByteStreamSplit(&enc.PlainInt64Encoder)
	if err != nil {
		return nil, err
	}
	encodeByteStreamSplitWidth8(out.Bytes(), in.Bytes())
	return out.Finish(), nil
}

// ByteStreamSplitFloat32Decoder is a decoder for BYTE_STREAM_SPLIT-encoded
// bytes representing Float32 values
type ByteStreamSplitFloat32Decoder struct {
	decoder
}

// Type returns the physical type this decoder operates on, Float32
func (ByteStreamSplitFloat32Decoder) Type() parquet.Type {
	return parquet.Types.Float
}

// Decode populates out with float32 values until either there are no more
// values to decode or the length of out has been filled. Then returns the total number of values
// that were decoded.
func (dec *ByteStreamSplitFloat32Decoder) Decode(out []float32) (int, error) {
	return decodeByteStreamSplitNumeric(out, dec, dec.data)
}

// DecodeSpaced does the same as Decode but spaces out the resulting slice according to the bitmap leaving space for null values
func (dec *ByteStreamSplitFloat32Decoder) DecodeSpaced(out []float32, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	return decodeByteStreamSplitSpaced(out, nullCount, validBits, validBitsOffset, dec.Decode)
}

// ByteStreamSplitFloat64Decoder is a decoder for BYTE_STREAM_SPLIT-encoded
// bytes representing Float64 values
type ByteStreamSplitFloat64Decoder struct {
	decoder
}

// Type returns the physical type this decoder operates on, Float64
func (ByteStreamSplitFloat64Decoder) Type() parquet.Type {
	return parquet.Types.Double
}

// Decode populates out with float64 values until either there are no more
// values to decode or the length of out has been filled. Then returns the total number of values
// that were decoded.
func (dec *ByteStreamSplitFloat64Decoder) Decode(out []float64) (int, error) {
	return decodeByteStreamSplitNumeric(out, dec, dec.data)
}

// DecodeSpaced does the same as Decode but spaces out the resulting slice according to the bitmap leaving space for null values
func (dec *ByteStreamSplitFloat64Decoder) DecodeSpaced(out []float64, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	return decodeByteStreamSplitSpaced(out, nullCount, validBits, validBitsOffset, dec.Decode)
}

// ByteStreamSplitInt32Decoder is a decoder for BYTE_STREAM_SPLIT-encoded
// bytes representing Int32 values
type ByteStreamSplitInt32Decoder struct {
	decoder
}

// Type returns the physical type this decoder operates on, Int32
func (ByteStreamSplitInt32Decoder) Type() parquet.Type {
	return parquet.Types.Int32
}

// Decode populates out with int32 values until either there are no more
// values to decode or the length of out has been filled. Then returns the total number of values
// that were decoded.
func (dec *ByteStreamSplitInt32Decoder) Decode(out []int32) (int, error) {
	return decodeByteStreamSplitNumeric(out, dec, dec.data)
}

// DecodeSpaced does the same as Decode but spaces out the resulting slice according to the bitmap leaving space for null values
func (dec *ByteStreamSplitInt32Decoder) DecodeSpaced(out []int32, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	return decodeByteStreamSplitSpaced(out, nullCount, validBits, validBitsOffset, dec.Decode)
}

// ByteStreamSplitInt64Decoder is a decoder for BYTE_STREAM_SPLIT-encoded
// bytes representing Int64 values
type ByteStreamSplitInt64Decoder struct {
	decoder
}

// Type returns the physical type this decoder operates on, Int64
func (ByteStreamSplitInt64Decoder) Type() parquet.Type {
	return parquet.Types.Int64
}

// Decode populates out with int64 values until either there are no more
// values to decode or the length of out has been filled. Then returns the total number of values
// that were decoded.
func (dec *ByteStreamSplitInt64Decoder) Decode(out []int64) (int, error) {
	return decodeByteStreamSplitNumeric(out, dec, dec.data)
}

// DecodeSpaced does the same as Decode but spaces out the resulting slice according to the bitmap leaving space for null values
func (dec *ByteStreamSplitInt64Decoder) DecodeSpaced(out []int64, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	return decodeByteStreamSplitSpaced(out, nullCount, validBits, validBitsOffset, dec.Decode)
}
