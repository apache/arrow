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
	"github.com/apache/arrow/go/v17/internal/bitutils"
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

// putByteStreamSplitNumeric is a generic implementation of the BYTE_STREAM_SPLIT encoding for fixed-width numeric types.
func putByteStreamSplitNumeric[T NumericByteStreamSplitType](in []T, enc TypedEncoder, sink *PooledBufferWriter) {
	numElements := len(in)
	typeLen := enc.Type().ByteSize()
	bytesNeeded := numElements * typeLen
	sink.Reserve(bytesNeeded)

	data := sink.buf.Bytes()
	data = data[sink.pos : sink.pos+bytesNeeded] // Get the chunk of memory we plan to write to

	inBytes := arrow.GetBytes(in)
	switch typeLen {
	case 4:
		encodeByteStreamSplitWidth4(data, inBytes, numElements)
	case 8:
		encodeByteStreamSplitWidth8(data, inBytes, numElements)
	default:
		encodeByteStreamSplit(data, inBytes, numElements, typeLen)
	}

	sink.pos += bytesNeeded
}

// encodeByteStreamSplit encodes the raw bytes provided by 'in' into the output buffer 'data' using BYTE_STREAM_SPLIT encoding
func encodeByteStreamSplit(data []byte, in []byte, numElements, width int) {
	for stream := 0; stream < width; stream++ {
		for element := 0; element < numElements; element++ {
			encLoc := numElements*stream + element
			decLoc := width*element + stream
			data[encLoc] = in[decLoc]
		}
	}
}

// encodeByteStreamSplitWidth4 implements encodeByteStreamSplit optimized for types stored using 4 bytes
func encodeByteStreamSplitWidth4(data []byte, in []byte, numElements int) {
	width := 4
	for element := 0; element < numElements; element++ {
		decLoc := width * element
		data[element] = in[decLoc]
		data[numElements+element] = in[decLoc+1]
		data[numElements*2+element] = in[decLoc+2]
		data[numElements*3+element] = in[decLoc+3]
	}
}

// encodeByteStreamSplitWidth8 implements encodeByteStreamSplit optimized for types stored using 8 bytes
func encodeByteStreamSplitWidth8(data []byte, in []byte, numElements int) {
	width := 8
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

// putByteStreamSplitSpaced encodes data that has space for nulls using the BYTE_STREAM_SPLIT encoding, calling to the provided putFn to encode runs of non-null values.
func putByteStreamSplitSpaced[T ByteStreamSplitType](in []T, validBits []byte, validBitsOffset int64, bitSetReader bitutils.SetBitRunReader, putFn func([]T)) {
	if validBits == nil {
		putFn(in)
		return
	}

	if bitSetReader == nil {
		bitSetReader = bitutils.NewSetBitRunReader(validBits, validBitsOffset, int64(len(in)))
	} else {
		bitSetReader.Reset(validBits, validBitsOffset, int64(len(in)))
	}

	for {
		run := bitSetReader.NextRun()
		if run.Length == 0 {
			break
		}
		putFn(in[int(run.Pos):int(run.Pos+run.Length)])
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

// ByteStreamSplitFloat32Encoder writes the underlying bytes of the Float32
// into interlaced streams as defined by the BYTE_STREAM_SPLIT encoding
type ByteStreamSplitFloat32Encoder struct {
	encoder
	bitSetReader bitutils.SetBitRunReader
}

// Put writes the provided values to the encoder
func (enc *ByteStreamSplitFloat32Encoder) Put(in []float32) {
	putByteStreamSplitNumeric(in, enc, enc.sink)
}

// PutSpaced is like Put but works with data that is spaced out according to the passed in bitmap
func (enc *ByteStreamSplitFloat32Encoder) PutSpaced(in []float32, validBits []byte, validBitsOffset int64) {
	putByteStreamSplitSpaced(in, validBits, validBitsOffset, enc.bitSetReader, enc.Put)
}

// Type returns the underlying physical type this encoder works with, Float32.
func (enc ByteStreamSplitFloat32Encoder) Type() parquet.Type {
	return parquet.Types.Float
}

// ByteStreamSplitFloat64Encoder writes the underlying bytes of the Float64
// into interlaced streams as defined by the BYTE_STREAM_SPLIT encoding
type ByteStreamSplitFloat64Encoder struct {
	encoder
	bitSetReader bitutils.SetBitRunReader
}

// Put writes the provided values to the encoder
func (enc *ByteStreamSplitFloat64Encoder) Put(in []float64) {
	putByteStreamSplitNumeric(in, enc, enc.sink)
}

// PutSpaced is like Put but works with data that is spaced out according to the passed in bitmap
func (enc *ByteStreamSplitFloat64Encoder) PutSpaced(in []float64, validBits []byte, validBitsOffset int64) {
	putByteStreamSplitSpaced(in, validBits, validBitsOffset, enc.bitSetReader, enc.Put)
}

// Type returns the underlying physical type this encoder works with, Float64.
func (enc ByteStreamSplitFloat64Encoder) Type() parquet.Type {
	return parquet.Types.Double
}

// ByteStreamSplitInt32Encoder writes the underlying bytes of the Int32
// into interlaced streams as defined by the BYTE_STREAM_SPLIT encoding
type ByteStreamSplitInt32Encoder struct {
	encoder
	bitSetReader bitutils.SetBitRunReader
}

// Put writes the provided values to the encoder
func (enc *ByteStreamSplitInt32Encoder) Put(in []int32) {
	putByteStreamSplitNumeric(in, enc, enc.sink)
}

// PutSpaced is like Put but works with data that is spaced out according to the passed in bitmap
func (enc *ByteStreamSplitInt32Encoder) PutSpaced(in []int32, validBits []byte, validBitsOffset int64) {
	putByteStreamSplitSpaced(in, validBits, validBitsOffset, enc.bitSetReader, enc.Put)
}

// Type returns the underlying physical type this encoder works with, Int32.
func (enc ByteStreamSplitInt32Encoder) Type() parquet.Type {
	return parquet.Types.Int32
}

// ByteStreamSplitInt64Encoder writes the underlying bytes of the Int64
// into interlaced streams as defined by the BYTE_STREAM_SPLIT encoding
type ByteStreamSplitInt64Encoder struct {
	encoder
	bitSetReader bitutils.SetBitRunReader
}

// Put writes the provided values to the encoder
func (enc *ByteStreamSplitInt64Encoder) Put(in []int64) {
	putByteStreamSplitNumeric(in, enc, enc.sink)
}

// PutSpaced is like Put but works with data that is spaced out according to the passed in bitmap
func (enc *ByteStreamSplitInt64Encoder) PutSpaced(in []int64, validBits []byte, validBitsOffset int64) {
	putByteStreamSplitSpaced(in, validBits, validBitsOffset, enc.bitSetReader, enc.Put)
}

// Type returns the underlying physical type this encoder works with, Int64.
func (enc ByteStreamSplitInt64Encoder) Type() parquet.Type {
	return parquet.Types.Int64
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
