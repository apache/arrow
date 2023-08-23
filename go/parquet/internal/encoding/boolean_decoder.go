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
	"github.com/apache/arrow/go/v14/arrow/bitutil"
	shared_utils "github.com/apache/arrow/go/v14/internal/utils"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/internal/utils"
	"golang.org/x/xerrors"
)

// PlainBooleanDecoder is for the Plain Encoding type, there is no
// dictionary decoding for bools.
type PlainBooleanDecoder struct {
	decoder

	bitOffset int
}

// Type for the PlainBooleanDecoder is parquet.Types.Boolean
func (PlainBooleanDecoder) Type() parquet.Type {
	return parquet.Types.Boolean
}

func (dec *PlainBooleanDecoder) SetData(nvals int, data []byte) error {
	if err := dec.decoder.SetData(nvals, data); err != nil {
		return err
	}
	dec.bitOffset = 0
	return nil
}

// Decode fills out with bools decoded from the data at the current point
// or until we reach the end of the data.
//
// Returns the number of values decoded
func (dec *PlainBooleanDecoder) Decode(out []bool) (int, error) {
	max := shared_utils.MinInt(len(out), dec.nvals)

	// attempts to read all remaining bool values from the current data byte
	unalignedExtract := func(i int) int {
		for ; dec.bitOffset < 8 && i < max; i, dec.bitOffset = i+1, dec.bitOffset+1 {
			out[i] = (dec.data[0] & byte(1<<dec.bitOffset)) != 0
		}
		if dec.bitOffset == 8 {
			// we read every bit from this byte
			dec.bitOffset = 0
			dec.data = dec.data[1:] // move data forward
		}
		return i // return the next index for out[]
	}

	// if we aren't at a byte boundary, then get bools until we hit
	// a byte boundary with the bit offset.
	i := 0
	if dec.bitOffset != 0 {
		i = unalignedExtract(i)
	}

	// determine the number of full bytes worth of bits we can decode
	// given the number of values we want to decode.
	bitsRemain := max - i
	batch := (bitsRemain / 8) * 8
	if batch > 0 { // only go in here if there's at least one full byte to decode
		// determine the number of aligned bytes we can grab using SIMD optimized
		// functions to improve performance.
		alignedBytes := bitutil.BytesForBits(int64(batch))
		utils.BytesToBools(dec.data[:alignedBytes], out[i:])

		dec.data = dec.data[alignedBytes:] // move data forward
		i += int(alignedBytes) * 8
	}

	// grab any trailing bits now that we've got our aligned bytes.
	_ = unalignedExtract(i)

	dec.nvals -= max
	return max, nil
}

// DecodeSpaced is like Decode except it expands the values to leave spaces for null
// as determined by the validBits bitmap.
func (dec *PlainBooleanDecoder) DecodeSpaced(out []bool, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	if nullCount > 0 {
		toRead := len(out) - nullCount
		valuesRead, err := dec.Decode(out[:toRead])
		if err != nil {
			return 0, err
		}
		if valuesRead != toRead {
			return valuesRead, xerrors.New("parquet: boolean decoder: number of values / definition levels read did not match")
		}
		return spacedExpand(out, nullCount, validBits, validBitsOffset), nil
	}
	return dec.Decode(out)
}
