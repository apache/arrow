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
	"math"
	"math/bits"
	"reflect"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/memory"
	shared_utils "github.com/apache/arrow/go/v13/internal/utils"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/internal/utils"
	"golang.org/x/xerrors"
)

// see the deltaBitPack encoder for a description of the encoding format that is
// used for delta-bitpacking.
type deltaBitPackDecoder struct {
	decoder

	mem memory.Allocator

	usedFirst            bool
	bitdecoder           *utils.BitReader
	blockSize            uint64
	currentBlockVals     uint32
	miniBlocks           uint64
	valsPerMini          uint32
	currentMiniBlockVals uint32
	minDelta             int64
	miniBlockIdx         uint64

	deltaBitWidths *memory.Buffer
	deltaBitWidth  byte

	totalValues uint64
	lastVal     int64
}

// returns the number of bytes read so far
func (d *deltaBitPackDecoder) bytesRead() int64 {
	return d.bitdecoder.CurOffset()
}

func (d *deltaBitPackDecoder) Allocator() memory.Allocator { return d.mem }

// SetData sets the bytes and the expected number of values to decode
// into the decoder, updating the decoder and allowing it to be reused.
func (d *deltaBitPackDecoder) SetData(nvalues int, data []byte) error {
	// set our data into the underlying decoder for the type
	if err := d.decoder.SetData(nvalues, data); err != nil {
		return err
	}
	// create a bit reader for our decoder's values
	d.bitdecoder = utils.NewBitReader(bytes.NewReader(d.data))
	d.currentBlockVals = 0
	d.currentMiniBlockVals = 0
	if d.deltaBitWidths == nil {
		d.deltaBitWidths = memory.NewResizableBuffer(d.mem)
	}

	var ok bool
	d.blockSize, ok = d.bitdecoder.GetVlqInt()
	if !ok {
		return xerrors.New("parquet: eof exception")
	}

	if d.miniBlocks, ok = d.bitdecoder.GetVlqInt(); !ok {
		return xerrors.New("parquet: eof exception")
	}

	if d.totalValues, ok = d.bitdecoder.GetVlqInt(); !ok {
		return xerrors.New("parquet: eof exception")
	}

	if d.lastVal, ok = d.bitdecoder.GetZigZagVlqInt(); !ok {
		return xerrors.New("parquet: eof exception")
	}

	if d.miniBlocks != 0 {
		d.valsPerMini = uint32(d.blockSize / d.miniBlocks)
	}
	return nil
}

// initialize a block to decode
func (d *deltaBitPackDecoder) initBlock() error {
	// first we grab the min delta value that we'll start from
	var ok bool
	if d.minDelta, ok = d.bitdecoder.GetZigZagVlqInt(); !ok {
		return xerrors.New("parquet: eof exception")
	}

	// ensure we have enough space for our miniblocks to decode the widths
	d.deltaBitWidths.Resize(int(d.miniBlocks))

	var err error
	for i := uint64(0); i < d.miniBlocks; i++ {
		if d.deltaBitWidths.Bytes()[i], err = d.bitdecoder.ReadByte(); err != nil {
			return err
		}
	}

	d.miniBlockIdx = 0
	d.deltaBitWidth = d.deltaBitWidths.Bytes()[0]
	d.currentBlockVals = uint32(d.blockSize)
	return nil
}

// DeltaBitPackInt32Decoder decodes Int32 values which are packed using the Delta BitPacking algorithm.
type DeltaBitPackInt32Decoder struct {
	*deltaBitPackDecoder

	miniBlockValues []int32
}

func (d *DeltaBitPackInt32Decoder) unpackNextMini() error {
	if d.miniBlockValues == nil {
		d.miniBlockValues = make([]int32, 0, int(d.valsPerMini))
	} else {
		d.miniBlockValues = d.miniBlockValues[:0]
	}
	d.deltaBitWidth = d.deltaBitWidths.Bytes()[int(d.miniBlockIdx)]
	d.currentMiniBlockVals = d.valsPerMini

	for j := 0; j < int(d.valsPerMini); j++ {
		delta, ok := d.bitdecoder.GetValue(int(d.deltaBitWidth))
		if !ok {
			return xerrors.New("parquet: eof exception")
		}

		d.lastVal += int64(delta) + int64(d.minDelta)
		d.miniBlockValues = append(d.miniBlockValues, int32(d.lastVal))
	}
	d.miniBlockIdx++
	return nil
}

// Decode retrieves min(remaining values, len(out)) values from the data and returns the number
// of values actually decoded and any errors encountered.
func (d *DeltaBitPackInt32Decoder) Decode(out []int32) (int, error) {
	max := shared_utils.MinInt(len(out), d.nvals)
	if max == 0 {
		return 0, nil
	}

	out = out[:max]
	if !d.usedFirst { // starting value to calculate deltas against
		out[0] = int32(d.lastVal)
		out = out[1:]
		d.usedFirst = true
	}

	var err error
	for len(out) > 0 { // unpack mini blocks until we get all the values we need
		if d.currentBlockVals == 0 {
			err = d.initBlock()
		}
		if d.currentMiniBlockVals == 0 {
			err = d.unpackNextMini()
		}
		if err != nil {
			return 0, err
		}

		// copy as many values from our mini block as we can into out
		start := int(d.valsPerMini - d.currentMiniBlockVals)
		numCopied := copy(out, d.miniBlockValues[start:])

		out = out[numCopied:]
		d.currentBlockVals -= uint32(numCopied)
		d.currentMiniBlockVals -= uint32(numCopied)
	}
	d.nvals -= max
	return max, nil
}

// DecodeSpaced is like Decode, but the result is spaced out appropriately based on the passed in bitmap
func (d *DeltaBitPackInt32Decoder) DecodeSpaced(out []int32, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
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

// Type returns the physical parquet type that this decoder decodes, in this case Int32
func (DeltaBitPackInt32Decoder) Type() parquet.Type {
	return parquet.Types.Int32
}

// DeltaBitPackInt64Decoder decodes a delta bit packed int64 column of data.
type DeltaBitPackInt64Decoder struct {
	*deltaBitPackDecoder

	miniBlockValues []int64
}

func (d *DeltaBitPackInt64Decoder) unpackNextMini() error {
	if d.miniBlockValues == nil {
		d.miniBlockValues = make([]int64, 0, int(d.valsPerMini))
	} else {
		d.miniBlockValues = d.miniBlockValues[:0]
	}

	d.deltaBitWidth = d.deltaBitWidths.Bytes()[int(d.miniBlockIdx)]
	d.currentMiniBlockVals = d.valsPerMini

	for j := 0; j < int(d.valsPerMini); j++ {
		delta, ok := d.bitdecoder.GetValue(int(d.deltaBitWidth))
		if !ok {
			return xerrors.New("parquet: eof exception")
		}

		d.lastVal += int64(delta) + int64(d.minDelta)
		d.miniBlockValues = append(d.miniBlockValues, d.lastVal)
	}
	d.miniBlockIdx++
	return nil
}

// Decode retrieves min(remaining values, len(out)) values from the data and returns the number
// of values actually decoded and any errors encountered.
func (d *DeltaBitPackInt64Decoder) Decode(out []int64) (int, error) {
	max := shared_utils.MinInt(len(out), d.nvals)
	if max == 0 {
		return 0, nil
	}

	out = out[:max]
	if !d.usedFirst {
		out[0] = d.lastVal
		out = out[1:]
		d.usedFirst = true
	}

	var err error
	for len(out) > 0 {
		if d.currentBlockVals == 0 {
			err = d.initBlock()
		}
		if d.currentMiniBlockVals == 0 {
			err = d.unpackNextMini()
		}

		if err != nil {
			return 0, err
		}

		start := int(d.valsPerMini - d.currentMiniBlockVals)
		numCopied := copy(out, d.miniBlockValues[start:])

		out = out[numCopied:]
		d.currentBlockVals -= uint32(numCopied)
		d.currentMiniBlockVals -= uint32(numCopied)
	}
	d.nvals -= max
	return max, nil
}

// Type returns the physical parquet type that this decoder decodes, in this case Int64
func (DeltaBitPackInt64Decoder) Type() parquet.Type {
	return parquet.Types.Int64
}

// DecodeSpaced is like Decode, but the result is spaced out appropriately based on the passed in bitmap
func (d DeltaBitPackInt64Decoder) DecodeSpaced(out []int64, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
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

const (
	// block size must be a multiple of 128
	defaultBlockSize     = 128
	defaultNumMiniBlocks = 4
	// block size / number of mini blocks must result in a multiple of 32
	defaultNumValuesPerMini = 32
	// max size of the header for the delta blocks
	maxHeaderWriterSize = 32
)

// deltaBitPackEncoder is an encoder for the DeltaBinary Packing format
// as per the parquet spec.
//
// Consists of a header followed by blocks of delta encoded values binary packed.
//
//	Format
// 		[header] [block 1] [block 2] ... [block N]
//
//	Header
//		[block size] [number of mini blocks per block] [total value count] [first value]
//
//	Block
//		[min delta] [list of bitwidths of the miniblocks] [miniblocks...]
//
// Sets aside bytes at the start of the internal buffer where the header will be written,
// and only writes the header when FlushValues is called before returning it.
type deltaBitPackEncoder struct {
	encoder

	bitWriter  *utils.BitWriter
	totalVals  uint64
	firstVal   int64
	currentVal int64

	blockSize     uint64
	miniBlockSize uint64
	numMiniBlocks uint64
	deltas        []int64
}

// flushBlock flushes out a finished block for writing to the underlying encoder
func (enc *deltaBitPackEncoder) flushBlock() {
	if len(enc.deltas) == 0 {
		return
	}

	// determine the minimum delta value
	minDelta := int64(math.MaxInt64)
	for _, delta := range enc.deltas {
		if delta < minDelta {
			minDelta = delta
		}
	}

	enc.bitWriter.WriteZigZagVlqInt(minDelta)
	// reserve enough bytes to write out our miniblock deltas
	offset := enc.bitWriter.ReserveBytes(int(enc.numMiniBlocks))

	valuesToWrite := int64(len(enc.deltas))
	for i := 0; i < int(enc.numMiniBlocks); i++ {
		n := shared_utils.Min(int64(enc.miniBlockSize), valuesToWrite)
		if n == 0 {
			break
		}

		maxDelta := int64(math.MinInt64)
		start := i * int(enc.miniBlockSize)
		for _, val := range enc.deltas[start : start+int(n)] {
			maxDelta = shared_utils.Max(maxDelta, val)
		}

		// compute bit width to store (max_delta - min_delta)
		width := uint(bits.Len64(uint64(maxDelta - minDelta)))
		// write out the bit width we used into the bytes we reserved earlier
		enc.bitWriter.WriteAt([]byte{byte(width)}, int64(offset+i))

		// write out our deltas
		for _, val := range enc.deltas[start : start+int(n)] {
			enc.bitWriter.WriteValue(uint64(val-minDelta), width)
		}

		valuesToWrite -= n

		// pad the last block if n < miniBlockSize
		for ; n < int64(enc.miniBlockSize); n++ {
			enc.bitWriter.WriteValue(0, width)
		}
	}
	enc.deltas = enc.deltas[:0]
}

// putInternal is the implementation for actually writing data which must be
// integral data as int, int8, int32, or int64.
func (enc *deltaBitPackEncoder) putInternal(data interface{}) {
	v := reflect.ValueOf(data)
	if v.Len() == 0 {
		return
	}

	idx := 0
	if enc.totalVals == 0 {
		enc.blockSize = defaultBlockSize
		enc.numMiniBlocks = defaultNumMiniBlocks
		enc.miniBlockSize = defaultNumValuesPerMini

		enc.firstVal = v.Index(0).Int()
		enc.currentVal = enc.firstVal
		idx = 1

		enc.bitWriter = utils.NewBitWriter(enc.sink)
	}

	enc.totalVals += uint64(v.Len())
	for ; idx < v.Len(); idx++ {
		val := v.Index(idx).Int()
		enc.deltas = append(enc.deltas, val-enc.currentVal)
		enc.currentVal = val
		if len(enc.deltas) == int(enc.blockSize) {
			enc.flushBlock()
		}
	}
}

// FlushValues flushes any remaining data and returns the finished encoded buffer
// or returns nil and any error encountered during flushing.
func (enc *deltaBitPackEncoder) FlushValues() (Buffer, error) {
	if enc.bitWriter != nil {
		// write any remaining values
		enc.flushBlock()
		enc.bitWriter.Flush(true)
	} else {
		enc.blockSize = defaultBlockSize
		enc.numMiniBlocks = defaultNumMiniBlocks
		enc.miniBlockSize = defaultNumValuesPerMini
	}

	buffer := make([]byte, maxHeaderWriterSize)
	headerWriter := utils.NewBitWriter(utils.NewWriterAtBuffer(buffer))

	headerWriter.WriteVlqInt(uint64(enc.blockSize))
	headerWriter.WriteVlqInt(uint64(enc.numMiniBlocks))
	headerWriter.WriteVlqInt(uint64(enc.totalVals))
	headerWriter.WriteZigZagVlqInt(int64(enc.firstVal))
	headerWriter.Flush(false)

	buffer = buffer[:headerWriter.Written()]
	enc.totalVals = 0

	if enc.bitWriter != nil {
		flushed := enc.sink.Finish()
		defer flushed.Release()

		buffer = append(buffer, flushed.Buf()[:enc.bitWriter.Written()]...)
	}
	return poolBuffer{memory.NewBufferBytes(buffer)}, nil
}

// EstimatedDataEncodedSize returns the current amount of data actually flushed out and written
func (enc *deltaBitPackEncoder) EstimatedDataEncodedSize() int64 {
	return int64(enc.bitWriter.Written())
}

// DeltaBitPackInt32Encoder is an encoder for the delta bitpacking encoding for int32 data.
type DeltaBitPackInt32Encoder struct {
	*deltaBitPackEncoder
}

// Put writes the values from the provided slice of int32 to the encoder
func (enc DeltaBitPackInt32Encoder) Put(in []int32) {
	enc.putInternal(in)
}

// PutSpaced takes a slice of int32 along with a bitmap that describes the nulls and an offset into the bitmap
// in order to write spaced data to the encoder.
func (enc DeltaBitPackInt32Encoder) PutSpaced(in []int32, validBits []byte, validBitsOffset int64) {
	buffer := memory.NewResizableBuffer(enc.mem)
	buffer.Reserve(arrow.Int32Traits.BytesRequired(len(in)))
	defer buffer.Release()

	data := arrow.Int32Traits.CastFromBytes(buffer.Buf())
	nvalid := spacedCompress(in, data, validBits, validBitsOffset)
	enc.Put(data[:nvalid])
}

// Type returns the underlying physical type this encoder works with, in this case Int32
func (DeltaBitPackInt32Encoder) Type() parquet.Type {
	return parquet.Types.Int32
}

// DeltaBitPackInt32Encoder is an encoder for the delta bitpacking encoding for int32 data.
type DeltaBitPackInt64Encoder struct {
	*deltaBitPackEncoder
}

// Put writes the values from the provided slice of int64 to the encoder
func (enc DeltaBitPackInt64Encoder) Put(in []int64) {
	enc.putInternal(in)
}

// PutSpaced takes a slice of int64 along with a bitmap that describes the nulls and an offset into the bitmap
// in order to write spaced data to the encoder.
func (enc DeltaBitPackInt64Encoder) PutSpaced(in []int64, validBits []byte, validBitsOffset int64) {
	buffer := memory.NewResizableBuffer(enc.mem)
	buffer.Reserve(arrow.Int64Traits.BytesRequired(len(in)))
	defer buffer.Release()

	data := arrow.Int64Traits.CastFromBytes(buffer.Buf())
	nvalid := spacedCompress(in, data, validBits, validBitsOffset)
	enc.Put(data[:nvalid])
}

// Type returns the underlying physical type this encoder works with, in this case Int64
func (DeltaBitPackInt64Encoder) Type() parquet.Type {
	return parquet.Types.Int64
}
