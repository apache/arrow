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
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/parquet"
	"github.com/apache/arrow/go/parquet/internal/utils"
)

const boolBufSize = 1024

// PlainBooleanEncoder encodes bools as a bitmap as per the Plain Encoding
type PlainBooleanEncoder struct {
	encoder
	nbits      int
	bitsBuffer []byte
}

// Type for the PlainBooleanEncoder is parquet.Types.Boolean
func (PlainBooleanEncoder) Type() parquet.Type {
	return parquet.Types.Boolean
}

// Put encodes the contents of in into the underlying data buffer.
func (enc *PlainBooleanEncoder) Put(in []bool) {
	if enc.bitsBuffer == nil {
		enc.bitsBuffer = make([]byte, boolBufSize)
	}

	bitOffset := 0
	// first check if we are in the middle of a byte due to previous
	// encoding of data and finish out that byte's bits.
	if enc.nbits > 0 {
		bitsToWrite := utils.MinInt(enc.nbits, len(in))
		beg := (boolBufSize * 8) - enc.nbits
		for i, val := range in[:bitsToWrite] {
			bitmask := uint8(1 << uint((beg+i)%8))
			if val {
				enc.bitsBuffer[(beg+i)/8] |= bitmask
			} else {
				enc.bitsBuffer[(beg+i)/8] &= bitmask ^ 0xFF
			}
		}
		enc.nbits -= bitsToWrite
		bitOffset = bitsToWrite
		if enc.nbits == 0 {
			enc.append(enc.bitsBuffer)
		}
	}

	// now that we're aligned, write the rest of our bits
	bitsRemain := len(in) - bitOffset
	for bitOffset < len(in) {
		enc.nbits = boolBufSize * 8
		bitsToWrite := utils.MinInt(bitsRemain, enc.nbits)
		for i, val := range in[bitOffset : bitOffset+bitsToWrite] {
			bitmask := uint8(1 << uint(i%8))
			if val {
				enc.bitsBuffer[i/8] |= bitmask
			} else {
				enc.bitsBuffer[i/8] &= bitmask ^ 0xFF
			}
		}
		bitOffset += bitsToWrite
		enc.nbits -= bitsToWrite
		bitsRemain -= bitsToWrite
		if enc.nbits == 0 {
			enc.append(enc.bitsBuffer)
		}
	}
}

// PutSpaced will use the validBits bitmap to determine which values are nulls
// and can be left out from the slice, and the encoded without those nulls.
func (enc *PlainBooleanEncoder) PutSpaced(in []bool, validBits []byte, validBitsOffset int64) {
	bufferOut := make([]bool, len(in))
	nvalid := spacedCompress(in, bufferOut, validBits, validBitsOffset)
	enc.Put(bufferOut[:nvalid])
}

// EstimatedDataEncodedSize returns the current number of bytes that have
// been buffered so far
func (enc *PlainBooleanEncoder) EstimatedDataEncodedSize() int64 {
	return int64(enc.sink.Len() + (boolBufSize * 8) - enc.nbits)
}

// FlushValues returns the buffered data, the responsibility is on the caller
// to release the buffer memory
func (enc *PlainBooleanEncoder) FlushValues() Buffer {
	if enc.nbits > 0 {
		toFlush := (boolBufSize * 8) - enc.nbits
		enc.append(enc.bitsBuffer[:bitutil.BytesForBits(int64(toFlush))])
		enc.nbits = boolBufSize * 8
	}

	return enc.sink.Finish()
}
