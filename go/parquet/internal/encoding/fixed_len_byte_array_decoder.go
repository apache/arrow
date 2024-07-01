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

	"github.com/apache/arrow/go/v17/internal/utils"
	"github.com/apache/arrow/go/v17/parquet"
	"golang.org/x/xerrors"
)

// PlainFixedLenByteArrayDecoder is a plain encoding decoder for Fixed Length Byte Arrays
type PlainFixedLenByteArrayDecoder struct {
	decoder
}

// Type returns the physical type this decoder operates on, FixedLength Byte Arrays
func (PlainFixedLenByteArrayDecoder) Type() parquet.Type {
	return parquet.Types.FixedLenByteArray
}

// Decode populates out with fixed length byte array values until either there are no more
// values to decode or the length of out has been filled. Then returns the total number of values
// that were decoded.
func (pflba *PlainFixedLenByteArrayDecoder) Decode(out []parquet.FixedLenByteArray) (int, error) {
	max := utils.Min(len(out), pflba.nvals)
	numBytesNeeded := max * pflba.typeLen
	if numBytesNeeded > len(pflba.data) || numBytesNeeded > math.MaxInt32 {
		return 0, xerrors.New("parquet: eof exception")
	}

	for idx := range out[:max] {
		out[idx] = pflba.data[:pflba.typeLen]
		pflba.data = pflba.data[pflba.typeLen:]
	}
	return max, nil
}

// DecodeSpaced does the same as Decode but spaces out the resulting slice according to the bitmap leaving space for null values
func (pflba *PlainFixedLenByteArrayDecoder) DecodeSpaced(out []parquet.FixedLenByteArray, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	toRead := len(out) - nullCount
	valuesRead, err := pflba.Decode(out[:toRead])
	if err != nil {
		return valuesRead, err
	}
	if valuesRead != toRead {
		return valuesRead, xerrors.New("parquet: number of values / definitions levels read did not match")
	}

	return spacedExpand(out, nullCount, validBits, validBitsOffset), nil
}

// ByteStreamSplitFixedLenByteArrayDecoder is a decoder for BYTE_STREAM_SPLIT-encoded
// bytes representing FixedLenByteArray values
type ByteStreamSplitFixedLenByteArrayDecoder struct {
	decoder
}

// Type returns the physical type this decoder operates on, FixedLength Byte Arrays
func (ByteStreamSplitFixedLenByteArrayDecoder) Type() parquet.Type {
	return parquet.Types.FixedLenByteArray
}

// Decode populates out with fixed length byte array values until either there are no more
// values to decode or the length of out has been filled. Then returns the total number of values
// that were decoded.
func (dec *ByteStreamSplitFixedLenByteArrayDecoder) Decode(out []parquet.FixedLenByteArray) (int, error) {
	max := utils.Min(len(out), dec.ValuesLeft())
	numBytesNeeded := max * dec.typeLen
	if numBytesNeeded > len(dec.data) || numBytesNeeded > math.MaxInt32 {
		return 0, xerrors.New("parquet: eof exception")
	}

	for idx := range out[:max] {
		if len(out[idx]) != dec.typeLen {
			out[idx] = make([]byte, dec.typeLen)
		}
	}

	switch dec.typeLen {
	case 2:
		decodeByteStreamSplitFixedLenByteArrayWidth2(dec.data, out, max)
	case 4:
		decodeByteStreamSplitFixedLenByteArrayWidth4(dec.data, out, max)
	case 8:
		decodeByteStreamSplitFixedLenByteArrayWidth8(dec.data, out, max)
	default:
		decodeByteStreamSplitFixedLenByteArray(dec.data, out, max, dec.typeLen)
	}

	dec.SetData(dec.ValuesLeft()-max, dec.data[max:])

	return max, nil
}

// decodeByteStreamSplitFixedLenByteArray decodes the FixedLenByteArrays provided by 'out' into the output buffer 'data' using BYTE_STREAM_SPLIT encoding
func decodeByteStreamSplitFixedLenByteArray(data []byte, out []parquet.FixedLenByteArray, numElements, width int) {
	for offset := 0; offset < width; offset++ {
		for element := 0; element < numElements; element++ {
			encLoc := numElements*offset + element
			out[element][offset] = data[encLoc]
		}
	}
}

// decodeByteStreamSplitFixedLenByteArrayWidth2 implements decodeByteStreamSplitFixedLenByteArray optimized for types stored using 2 bytes
func decodeByteStreamSplitFixedLenByteArrayWidth2(data []byte, out []parquet.FixedLenByteArray, numElements int) {
	for element := 0; element < numElements; element++ {
		out[element][0] = data[element]
		out[element][1] = data[numElements+element]
	}
}

// decodeByteStreamSplitFixedLenByteArrayWidth4 implements decodeByteStreamSplitFixedLenByteArray optimized for types stored using 4 bytes
func decodeByteStreamSplitFixedLenByteArrayWidth4(data []byte, out []parquet.FixedLenByteArray, numElements int) {
	for element := 0; element < numElements; element++ {
		out[element][0] = data[element]
		out[element][1] = data[numElements+element]
		out[element][2] = data[numElements*2+element]
		out[element][3] = data[numElements*3+element]
	}
}

// decodeByteStreamSplitFixedLenByteArrayWidth8 implements decodeByteStreamSplitFixedLenByteArray optimized for types stored using 8 bytes
func decodeByteStreamSplitFixedLenByteArrayWidth8(data []byte, out []parquet.FixedLenByteArray, numElements int) {
	for element := 0; element < numElements; element++ {
		out[element][0] = data[element]
		out[element][1] = data[numElements+element]
		out[element][2] = data[numElements*2+element]
		out[element][3] = data[numElements*3+element]
		out[element][4] = data[numElements*4+element]
		out[element][5] = data[numElements*5+element]
		out[element][6] = data[numElements*6+element]
		out[element][7] = data[numElements*7+element]
	}
}

// DecodeSpaced does the same as Decode but spaces out the resulting slice according to the bitmap leaving space for null values
func (dec *ByteStreamSplitFixedLenByteArrayDecoder) DecodeSpaced(out []parquet.FixedLenByteArray, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	return decodeByteStreamSplitSpaced(out, nullCount, validBits, validBitsOffset, dec.Decode)
}
