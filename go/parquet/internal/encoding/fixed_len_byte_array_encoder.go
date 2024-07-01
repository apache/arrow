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
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/internal/bitutils"
	"github.com/apache/arrow/go/v17/parquet"
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

// ByteStreamSplitFixedLenByteArrayEncoder writes the underlying bytes of the FixedLenByteArray
// into interlaced streams as defined by the BYTE_STREAM_SPLIT encoding
type ByteStreamSplitFixedLenByteArrayEncoder struct {
	encoder

	bitSetReader bitutils.SetBitRunReader
}

// Put writes the provided values to the encoder
func (enc *ByteStreamSplitFixedLenByteArrayEncoder) Put(in []parquet.FixedLenByteArray) {
	numElements := len(in)
	bytesNeeded := numElements * enc.typeLen
	enc.sink.Reserve(bytesNeeded)

	data := enc.sink.buf.Bytes()
	data = data[:cap(data)] // Sets len = cap so we can index into any loc rather than append

	switch enc.typeLen {
	case 2:
		encodeByteStreamSplitFixedLenByteArrayWidth2(data, in, numElements)
	case 4:
		encodeByteStreamSplitFixedLenByteArrayWidth4(data, in, numElements)
	case 8:
		encodeByteStreamSplitFixedLenByteArrayWidth8(data, in, numElements)
	default:
		encodeByteStreamSplitFixedLenByteArray(data, in, numElements, enc.typeLen)
	}

	enc.sink.pos += bytesNeeded
}

// encodeByteStreamSplitFixedLenByteArray encodes the FixedLenByteArrays provided by 'in' into the output buffer 'data' using BYTE_STREAM_SPLIT encoding
func encodeByteStreamSplitFixedLenByteArray(data []byte, in []parquet.FixedLenByteArray, numElements, width int) {
	for offset := 0; offset < width; offset++ {
		for element := range in {
			encLoc := numElements*offset + element
			data[encLoc] = in[element][offset]
		}
	}
}

// encodeByteStreamSplitFixedLenByteArrayWidth2 implements encodeByteStreamSplitFixedLenByteArray optimized for types stored using 2 bytes
func encodeByteStreamSplitFixedLenByteArrayWidth2(data []byte, in []parquet.FixedLenByteArray, numElements int) {
	for element := range in {
		data[element] = in[element][0]
		data[numElements+element] = in[element][1]
	}
}

// encodeByteStreamSplitFixedLenByteArrayWidth4 implements encodeByteStreamSplitFixedLenByteArray optimized for types stored using 4 bytes
func encodeByteStreamSplitFixedLenByteArrayWidth4(data []byte, in []parquet.FixedLenByteArray, numElements int) {
	for element := range in {
		data[element] = in[element][0]
		data[numElements+element] = in[element][1]
		data[numElements*2+element] = in[element][2]
		data[numElements*3+element] = in[element][3]
	}
}

// encodeByteStreamSplitFixedLenByteArrayWidth8 implements encodeByteStreamSplitFixedLenByteArray optimized for types stored using 8 bytes
func encodeByteStreamSplitFixedLenByteArrayWidth8(data []byte, in []parquet.FixedLenByteArray, numElements int) {
	for element := range in {
		data[element] = in[element][0]
		data[numElements+element] = in[element][1]
		data[numElements*2+element] = in[element][2]
		data[numElements*3+element] = in[element][3]
		data[numElements*4+element] = in[element][4]
		data[numElements*5+element] = in[element][5]
		data[numElements*6+element] = in[element][6]
		data[numElements*7+element] = in[element][7]
	}
}

// PutSpaced is like Put but works with data that is spaced out according to the passed in bitmap
func (enc *ByteStreamSplitFixedLenByteArrayEncoder) PutSpaced(in []parquet.FixedLenByteArray, validBits []byte, validBitsOffset int64) {
	putByteStreamSplitSpaced(in, validBits, validBitsOffset, enc.bitSetReader, enc.Put)
}

// Type returns the underlying physical type this encoder works with, Fixed Length byte arrays.
func (ByteStreamSplitFixedLenByteArrayEncoder) Type() parquet.Type {
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

// PutDictionary allows pre-seeding a dictionary encoder with
// a dictionary from an Arrow Array.
//
// The passed in array must not have any nulls and this can only
// be called on an empty encoder.
func (enc *DictFixedLenByteArrayEncoder) PutDictionary(values arrow.Array) error {
	if values.DataType().ID() != arrow.FIXED_SIZE_BINARY && values.DataType().ID() != arrow.DECIMAL {
		return fmt.Errorf("%w: only fixed size binary and decimal128 arrays are supported", arrow.ErrInvalid)
	}

	if values.DataType().(arrow.FixedWidthDataType).Bytes() != enc.typeLen {
		return fmt.Errorf("%w: size mismatch: %s should have been %d wide",
			arrow.ErrInvalid, values.DataType(), enc.typeLen)
	}

	if err := enc.canPutDictionary(values); err != nil {
		return err
	}

	enc.dictEncodedSize += enc.typeLen * values.Len()
	data := values.Data().Buffers()[1].Bytes()[values.Data().Offset()*enc.typeLen:]
	for i := 0; i < values.Len(); i++ {
		_, _, err := enc.memo.GetOrInsert(data[i*enc.typeLen : (i+1)*enc.typeLen])
		if err != nil {
			return err
		}
	}

	values.Retain()
	enc.preservedDict = values
	return nil
}
