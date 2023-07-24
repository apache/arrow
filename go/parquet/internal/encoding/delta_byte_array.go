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
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/internal/utils"
	"github.com/apache/arrow/go/v13/parquet"
	"golang.org/x/xerrors"
)

// DeltaByteArrayEncoder is an encoder for writing bytearrays which are delta encoded
// this is also known as incremental encoding or front compression. For each element
// in a sequence of strings, we store the prefix length of the previous entry plus the suffix
// see https://en.wikipedia.org/wiki/Incremental_encoding for a longer description.
//
// This is stored as a sequence of delta-encoded prefix lengths followed by the suffixes
// encoded as delta length byte arrays.
type DeltaByteArrayEncoder struct {
	encoder

	prefixEncoder *DeltaBitPackInt32Encoder
	suffixEncoder *DeltaLengthByteArrayEncoder

	lastVal parquet.ByteArray
}

func (enc *DeltaByteArrayEncoder) EstimatedDataEncodedSize() int64 {
	return enc.prefixEncoder.EstimatedDataEncodedSize() + enc.suffixEncoder.EstimatedDataEncodedSize()
}

func (enc *DeltaByteArrayEncoder) initEncoders() {
	enc.prefixEncoder = &DeltaBitPackInt32Encoder{
		deltaBitPackEncoder: &deltaBitPackEncoder{encoder: newEncoderBase(enc.encoding, nil, enc.mem)}}
	enc.suffixEncoder = &DeltaLengthByteArrayEncoder{
		newEncoderBase(enc.encoding, nil, enc.mem),
		&DeltaBitPackInt32Encoder{
			deltaBitPackEncoder: &deltaBitPackEncoder{encoder: newEncoderBase(enc.encoding, nil, enc.mem)}}}
}

// Type returns the underlying physical type this operates on, in this case ByteArrays only
func (DeltaByteArrayEncoder) Type() parquet.Type { return parquet.Types.ByteArray }

// Put writes a slice of ByteArrays to the encoder
func (enc *DeltaByteArrayEncoder) Put(in []parquet.ByteArray) {
	if len(in) == 0 {
		return
	}

	var suf parquet.ByteArray
	if enc.prefixEncoder == nil { // initialize our encoders if we haven't yet
		enc.initEncoders()
		enc.prefixEncoder.Put([]int32{0})
		suf = in[0]
		enc.lastVal = in[0]
		enc.suffixEncoder.Put([]parquet.ByteArray{suf})
		in = in[1:]
	}

	// for each value, figure out the common prefix with the previous value
	// and then write the prefix length and the suffix.
	for _, val := range in {
		l1 := enc.lastVal.Len()
		l2 := val.Len()
		j := 0
		for j < l1 && j < l2 {
			if enc.lastVal[j] != val[j] {
				break
			}
			j++
		}
		enc.prefixEncoder.Put([]int32{int32(j)})
		suf = val[j:]
		enc.suffixEncoder.Put([]parquet.ByteArray{suf})
		enc.lastVal = val
	}

	// do the memcpy after the loops to keep a copy of the lastVal
	// we do a copy here so that we only copy and keep a reference
	// to the suffix, and aren't forcing the *entire* value to stay
	// in memory while we have this reference to just the suffix.
	enc.lastVal = append([]byte{}, enc.lastVal...)
}

// PutSpaced is like Put, but assumes the data is already spaced for nulls and uses the bitmap provided and offset
// to compress the data before writing it without the null slots.
func (enc *DeltaByteArrayEncoder) PutSpaced(in []parquet.ByteArray, validBits []byte, validBitsOffset int64) {
	if validBits != nil {
		data := make([]parquet.ByteArray, len(in))
		nvalid := spacedCompress(in, data, validBits, validBitsOffset)
		enc.Put(data[:nvalid])
	} else {
		enc.Put(in)
	}
}

// Flush flushes any remaining data out and returns the finished encoded buffer.
// or returns nil and any error encountered during flushing.
func (enc *DeltaByteArrayEncoder) FlushValues() (Buffer, error) {
	if enc.prefixEncoder == nil {
		enc.initEncoders()
	}
	prefixBuf, err := enc.prefixEncoder.FlushValues()
	if err != nil {
		return nil, err
	}
	defer prefixBuf.Release()

	suffixBuf, err := enc.suffixEncoder.FlushValues()
	if err != nil {
		return nil, err
	}
	defer suffixBuf.Release()

	ret := bufferPool.Get().(*memory.Buffer)
	ret.ResizeNoShrink(prefixBuf.Len() + suffixBuf.Len())
	copy(ret.Bytes(), prefixBuf.Bytes())
	copy(ret.Bytes()[prefixBuf.Len():], suffixBuf.Bytes())
	return poolBuffer{ret}, nil
}

// DeltaByteArrayDecoder is a decoder for a column of data encoded using incremental or prefix encoding.
type DeltaByteArrayDecoder struct {
	*DeltaLengthByteArrayDecoder

	prefixLengths []int32
	lastVal       parquet.ByteArray
}

// Type returns the underlying physical type this decoder operates on, in this case ByteArrays only
func (DeltaByteArrayDecoder) Type() parquet.Type {
	return parquet.Types.ByteArray
}

func (d *DeltaByteArrayDecoder) Allocator() memory.Allocator { return d.mem }

// SetData expects the data passed in to be the prefix lengths, followed by the
// blocks of suffix data in order to initialize the decoder.
func (d *DeltaByteArrayDecoder) SetData(nvalues int, data []byte) error {
	prefixLenDec := DeltaBitPackInt32Decoder{
		deltaBitPackDecoder: &deltaBitPackDecoder{
			decoder: newDecoderBase(d.encoding, d.descr),
			mem:     d.mem}}

	if err := prefixLenDec.SetData(nvalues, data); err != nil {
		return err
	}

	d.prefixLengths = make([]int32, nvalues)
	// decode all the prefix lengths first so we know how many bytes it took to get the
	// prefix lengths for nvalues
	prefixLenDec.Decode(d.prefixLengths)

	// now that we know how many bytes we needed for the prefix lengths, the rest are the
	// delta length byte array encoding.
	return d.DeltaLengthByteArrayDecoder.SetData(nvalues, data[int(prefixLenDec.bytesRead()):])
}

// Decode decodes byte arrays into the slice provided and returns the number of values actually decoded
func (d *DeltaByteArrayDecoder) Decode(out []parquet.ByteArray) (int, error) {
	max := utils.MinInt(len(out), d.nvals)
	if max == 0 {
		return 0, nil
	}
	out = out[:max]

	var err error
	if d.lastVal == nil {
		_, err = d.DeltaLengthByteArrayDecoder.Decode(out[:1])
		if err != nil {
			return 0, err
		}
		d.lastVal = out[0]
		out = out[1:]
		d.prefixLengths = d.prefixLengths[1:]
	}

	var prefixLen int32
	suffixHolder := make([]parquet.ByteArray, 1)
	for len(out) > 0 {
		prefixLen, d.prefixLengths = d.prefixLengths[0], d.prefixLengths[1:]

		prefix := d.lastVal[:prefixLen:prefixLen]
		_, err = d.DeltaLengthByteArrayDecoder.Decode(suffixHolder)
		if err != nil {
			return 0, err
		}

		if len(suffixHolder[0]) == 0 {
			d.lastVal = prefix
		} else {
			d.lastVal = make([]byte, int(prefixLen)+len(suffixHolder[0]))
			copy(d.lastVal, prefix)
			copy(d.lastVal[prefixLen:], suffixHolder[0])
		}
		out[0], out = d.lastVal, out[1:]
	}
	return max, nil
}

// DecodeSpaced is like decode, but the result is spaced out based on the bitmap provided.
func (d *DeltaByteArrayDecoder) DecodeSpaced(out []parquet.ByteArray, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	toread := len(out) - nullCount
	values, err := d.Decode(out[:toread])
	if err != nil {
		return values, err
	}
	if values != toread {
		return values, xerrors.New("parquet: number of values / definition levels read did not match")
	}

	return spacedExpand(out, nullCount, validBits, validBitsOffset), nil
}
