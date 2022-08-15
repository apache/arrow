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
	"github.com/apache/arrow/go/v10/internal/bitutils"
	"github.com/apache/arrow/go/v10/parquet"
)

// PlainFixedLenByteArrayEncoder writes the raw bytes of the byte array
// always writing typeLength bytes for each value.
type PlainFixedLenByteArrayEncoder struct {
	encoder

	bitSetReader bitutils.SetBitRunReader
}

// Put writes the provided values to the encoder
func (enc *PlainFixedLenByteArrayEncoder) Put(in []parquet.FixedLenByteArray) {
	typeLen := enc.descr.TypeLength()
	if typeLen == 0 {
		return
	}

	bytesNeeded := len(in) * typeLen
	enc.sink.Reserve(bytesNeeded)
	for _, val := range in {
		if val == nil {
			panic("value cannot be nil")
		}
		enc.sink.UnsafeWrite(val[:typeLen])
	}
}

// PutSpaced is like Put but works with data that is spaced out according to the passed in bitmap
func (enc *PlainFixedLenByteArrayEncoder) PutSpaced(in []parquet.FixedLenByteArray, validBits []byte, validBitsOffset int64) {
	if validBits != nil {
		if enc.bitSetReader == nil {
			enc.bitSetReader = bitutils.NewSetBitRunReader(validBits, validBitsOffset, int64(len(in)))
		} else {
			enc.bitSetReader.Reset(validBits, validBitsOffset, int64(len(in)))
		}

		for {
			run := enc.bitSetReader.NextRun()
			if run.Length == 0 {
				break
			}
			enc.Put(in[int(run.Pos):int(run.Pos+run.Length)])
		}
	} else {
		enc.Put(in)
	}
}

// Type returns the underlying physical type this encoder works with, Fixed Length byte arrays.
func (PlainFixedLenByteArrayEncoder) Type() parquet.Type {
	return parquet.Types.FixedLenByteArray
}

// WriteDict overrides the embedded WriteDict function to call a specialized function
// for copying out the Fixed length values from the dictionary more efficiently.
func (enc *DictFixedLenByteArrayEncoder) WriteDict(out []byte) {
	enc.memo.(BinaryMemoTable).CopyFixedWidthValues(0, enc.typeLen, out)
}

// Put writes fixed length values to a dictionary encoded column
func (enc *DictFixedLenByteArrayEncoder) Put(in []parquet.FixedLenByteArray) {
	for _, v := range in {
		if v == nil {
			v = empty[:]
		}
		memoIdx, found, err := enc.memo.GetOrInsert(v)
		if err != nil {
			panic(err)
		}
		if !found {
			enc.dictEncodedSize += enc.typeLen
		}
		enc.addIndex(memoIdx)
	}
}

// PutSpaced is like Put but leaves space for nulls
func (enc *DictFixedLenByteArrayEncoder) PutSpaced(in []parquet.FixedLenByteArray, validBits []byte, validBitsOffset int64) {
	bitutils.VisitSetBitRuns(validBits, validBitsOffset, int64(len(in)), func(pos, length int64) error {
		enc.Put(in[pos : pos+length])
		return nil
	})
}
