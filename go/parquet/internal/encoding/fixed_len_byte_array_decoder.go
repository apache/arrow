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

	"github.com/apache/arrow/go/v13/internal/utils"
	"github.com/apache/arrow/go/v13/parquet"
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
	max := utils.MinInt(len(out), pflba.nvals)
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
