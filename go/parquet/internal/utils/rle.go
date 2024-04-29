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

// Package utils contains various internal utilities for the parquet library
// that aren't intended to be exposed to external consumers such as interfaces
// and bitmap readers/writers including the RLE encoder/decoder and so on.
package utils

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/apache/arrow/go/v17/arrow/bitutil"
	"github.com/apache/arrow/go/v17/internal/bitutils"
	"github.com/apache/arrow/go/v17/internal/utils"
	"github.com/apache/arrow/go/v17/parquet"
	"golang.org/x/xerrors"
)

//go:generate go run ../../../arrow/_tools/tmpl/main.go -i -data=physical_types.tmpldata typed_rle_dict.gen.go.tmpl

const (
	MaxValuesPerLiteralRun = (1 << 6) * 8
)

func MinRLEBufferSize(bitWidth int) int {
	maxLiteralRunSize := 1 + bitutil.BytesForBits(int64(MaxValuesPerLiteralRun*bitWidth))
	maxRepeatedRunSize := binary.MaxVarintLen32 + bitutil.BytesForBits(int64(bitWidth))
	return int(utils.Max(maxLiteralRunSize, maxRepeatedRunSize))
}

func MaxRLEBufferSize(width, numValues int) int {
	bytesPerRun := width
	numRuns := int(bitutil.BytesForBits(int64(numValues)))
	literalMaxSize := numRuns + (numRuns * bytesPerRun)

	minRepeatedRunSize := 1 + int(bitutil.BytesForBits(int64(width)))
	repeatedMaxSize := int(bitutil.BytesForBits(int64(numValues))) * minRepeatedRunSize

	return utils.Max(literalMaxSize, repeatedMaxSize)
}

// Utility classes to do run length encoding (RLE) for fixed bit width values.  If runs
// are sufficiently long, RLE is used, otherwise, the values are just bit-packed
// (literal encoding).
// For both types of runs, there is a byte-aligned indicator which encodes the length
// of the run and the type of the run.
// This encoding has the benefit that when there aren't any long enough runs, values
// are always decoded at fixed (can be precomputed) bit offsets OR both the value and
// the run length are byte aligned. This allows for very efficient decoding
// implementations.
// The encoding is:
//    encoded-block := run*
//    run := literal-run | repeated-run
//    literal-run := literal-indicator < literal bytes >
//    repeated-run := repeated-indicator < repeated value. padded to byte boundary >
//    literal-indicator := varint_encode( number_of_groups << 1 | 1)
//    repeated-indicator := varint_encode( number_of_repetitions << 1 )
//
// Each run is preceded by a varint. The varint's least significant bit is
// used to indicate whether the run is a literal run or a repeated run. The rest
// of the varint is used to determine the length of the run (eg how many times the
// value repeats).
//
// In the case of literal runs, the run length is always a multiple of 8 (i.e. encode
// in groups of 8), so that no matter the bit-width of the value, the sequence will end
// on a byte boundary without padding.
// Given that we know it is a multiple of 8, we store the number of 8-groups rather than
// the actual number of encoded ints. (This means that the total number of encoded values
// cannot be determined from the encoded data, since the number of values in the last
// group may not be a multiple of 8). For the last group of literal runs, we pad
// the group to 8 with zeros. This allows for 8 at a time decoding on the read side
// without the need for additional checks.
//
// There is a break-even point when it is more storage efficient to do run length
// encoding.  For 1 bit-width values, that point is 8 values.  They require 2 bytes
// for both the repeated encoding or the literal encoding.  This value can always
// be computed based on the bit-width.
//
// Examples with bit-width 1 (eg encoding booleans):
// ----------------------------------------
// 100 1s followed by 100 0s:
// <varint(100 << 1)> <1, padded to 1 byte> <varint(100 << 1)> <0, padded to 1 byte>
//  - (total 4 bytes)
//
// alternating 1s and 0s (200 total):
// 200 ints = 25 groups of 8
// <varint((25 << 1) | 1)> <25 bytes of values, bitpacked>
// (total 26 bytes, 1 byte overhead)
//

type RleDecoder struct {
	r *BitReader

	bitWidth int
	curVal   uint64
	repCount int32
	litCount int32
}

func NewRleDecoder(data *bytes.Reader, width int) *RleDecoder {
	return &RleDecoder{r: NewBitReader(data), bitWidth: width}
}

func (r *RleDecoder) Reset(data *bytes.Reader, width int) {
	r.bitWidth = width
	r.curVal = 0
	r.repCount = 0
	r.litCount = 0
	r.r.Reset(data)
}

func (r *RleDecoder) Next() bool {
	indicator, ok := r.r.GetVlqInt()
	if !ok {
		return false
	}

	literal := (indicator & 1) != 0
	count := uint32(indicator >> 1)
	if literal {
		if count == 0 || count > uint32(math.MaxInt32/8) {
			return false
		}
		r.litCount = int32(count) * 8
	} else {
		if count == 0 || count > uint32(math.MaxInt32) {
			return false
		}
		r.repCount = int32(count)

		nbytes := int(bitutil.BytesForBits(int64(r.bitWidth)))
		switch {
		case nbytes > 4:
			if !r.r.GetAligned(nbytes, &r.curVal) {
				return false
			}
		case nbytes > 2:
			var val uint32
			if !r.r.GetAligned(nbytes, &val) {
				return false
			}
			r.curVal = uint64(val)
		case nbytes > 1:
			var val uint16
			if !r.r.GetAligned(nbytes, &val) {
				return false
			}
			r.curVal = uint64(val)
		default:
			var val uint8
			if !r.r.GetAligned(nbytes, &val) {
				return false
			}
			r.curVal = uint64(val)
		}
	}
	return true
}

func (r *RleDecoder) GetValue() (uint64, bool) {
	vals := make([]uint64, 1)
	n := r.GetBatch(vals)
	return vals[0], n == 1
}

func (r *RleDecoder) GetBatch(values []uint64) int {
	read := 0
	size := len(values)

	out := values
	for read < size {
		remain := size - read

		if r.repCount > 0 {
			repbatch := int(math.Min(float64(remain), float64(r.repCount)))
			for i := 0; i < repbatch; i++ {
				out[i] = r.curVal
			}

			r.repCount -= int32(repbatch)
			read += repbatch
			out = out[repbatch:]
		} else if r.litCount > 0 {
			litbatch := int(math.Min(float64(remain), float64(r.litCount)))
			n, _ := r.r.GetBatch(uint(r.bitWidth), out[:litbatch])
			if n != litbatch {
				return read
			}

			r.litCount -= int32(litbatch)
			read += litbatch
			out = out[litbatch:]
		} else {
			if !r.Next() {
				return read
			}
		}
	}
	return read
}

func (r *RleDecoder) GetBatchSpaced(vals []uint64, nullcount int, validBits []byte, validBitsOffset int64) (int, error) {
	if nullcount == 0 {
		return r.GetBatch(vals), nil
	}

	converter := plainConverter{}
	blockCounter := bitutils.NewBitBlockCounter(validBits, validBitsOffset, int64(len(vals)))

	var (
		totalProcessed int
		processed      int
		block          bitutils.BitBlockCount
		err            error
	)

	for {
		block = blockCounter.NextFourWords()
		if block.Len == 0 {
			break
		}

		if block.AllSet() {
			processed = r.GetBatch(vals[:block.Len])
		} else if block.NoneSet() {
			converter.FillZero(vals[:block.Len])
			processed = int(block.Len)
		} else {
			processed, err = r.getspaced(converter, vals, int(block.Len), int(block.Len-block.Popcnt), validBits, validBitsOffset)
			if err != nil {
				return totalProcessed, err
			}
		}

		totalProcessed += processed
		vals = vals[int(block.Len):]
		validBitsOffset += int64(block.Len)

		if processed != int(block.Len) {
			break
		}
	}
	return totalProcessed, nil
}

func (r *RleDecoder) getspaced(dc DictionaryConverter, vals interface{}, batchSize, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	switch vals := vals.(type) {
	case []int32:
		return r.getspacedInt32(dc, vals, batchSize, nullCount, validBits, validBitsOffset)
	case []int64:
		return r.getspacedInt64(dc, vals, batchSize, nullCount, validBits, validBitsOffset)
	case []float32:
		return r.getspacedFloat32(dc, vals, batchSize, nullCount, validBits, validBitsOffset)
	case []float64:
		return r.getspacedFloat64(dc, vals, batchSize, nullCount, validBits, validBitsOffset)
	case []parquet.ByteArray:
		return r.getspacedByteArray(dc, vals, batchSize, nullCount, validBits, validBitsOffset)
	case []parquet.FixedLenByteArray:
		return r.getspacedFixedLenByteArray(dc, vals, batchSize, nullCount, validBits, validBitsOffset)
	case []parquet.Int96:
		return r.getspacedInt96(dc, vals, batchSize, nullCount, validBits, validBitsOffset)
	case []uint64:
		return r.getspacedUint64(dc, vals, batchSize, nullCount, validBits, validBitsOffset)
	default:
		return 0, xerrors.New("parquet/rle: getspaced invalid type")
	}
}

func (r *RleDecoder) getspacedUint64(dc DictionaryConverter, vals []uint64, batchSize, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	if nullCount == batchSize {
		dc.FillZero(vals[:batchSize])
		return batchSize, nil
	}

	read := 0
	remain := batchSize - nullCount

	const bufferSize = 1024
	var indexbuffer [bufferSize]IndexType

	// assume no bits to start
	bitReader := bitutils.NewBitRunReader(validBits, validBitsOffset, int64(batchSize))
	validRun := bitReader.NextRun()
	for read < batchSize {
		if validRun.Len == 0 {
			validRun = bitReader.NextRun()
		}

		if !validRun.Set {
			dc.FillZero(vals[:int(validRun.Len)])
			vals = vals[int(validRun.Len):]
			read += int(validRun.Len)
			validRun.Len = 0
			continue
		}

		if r.repCount == 0 && r.litCount == 0 {
			if !r.Next() {
				return read, nil
			}
		}

		var batch int
		switch {
		case r.repCount > 0:
			batch, remain, validRun = r.consumeRepeatCounts(read, batchSize, remain, validRun, bitReader)
			current := IndexType(r.curVal)
			if !dc.IsValid(current) {
				return read, nil
			}
			dc.Fill(vals[:batch], current)
		case r.litCount > 0:
			var (
				litread int
				skipped int
				err     error
			)
			litread, skipped, validRun, err = r.consumeLiteralsUint64(dc, vals, remain, indexbuffer[:], validRun, bitReader)
			if err != nil {
				return read, err
			}
			batch = litread + skipped
			remain -= litread
		}

		vals = vals[batch:]
		read += batch
	}
	return read, nil
}

func (r *RleDecoder) consumeRepeatCounts(read, batchSize, remain int, run bitutils.BitRun, bitRdr bitutils.BitRunReader) (int, int, bitutils.BitRun) {
	// Consume the entire repeat counts incrementing repeat_batch to
	// be the total of nulls + values consumed, we only need to
	// get the total count because we can fill in the same value for
	// nulls and non-nulls. This proves to be a big efficiency win.
	repeatBatch := 0
	for r.repCount > 0 && (read+repeatBatch) < batchSize {
		if run.Set {
			updateSize := int(utils.Min(run.Len, int64(r.repCount)))
			r.repCount -= int32(updateSize)
			repeatBatch += updateSize
			run.Len -= int64(updateSize)
			remain -= updateSize
		} else {
			repeatBatch += int(run.Len)
			run.Len = 0
		}

		if run.Len == 0 {
			run = bitRdr.NextRun()
		}
	}
	return repeatBatch, remain, run
}

func (r *RleDecoder) consumeLiteralsUint64(dc DictionaryConverter, vals []uint64, remain int, buf []IndexType, run bitutils.BitRun, bitRdr bitutils.BitRunReader) (int, int, bitutils.BitRun, error) {
	batch := utils.Min(utils.Min(remain, int(r.litCount)), len(buf))
	buf = buf[:batch]

	n, _ := r.r.GetBatchIndex(uint(r.bitWidth), buf)
	if n != batch {
		return 0, 0, run, xerrors.New("was not able to retrieve correct number of indexes")
	}

	if !dc.IsValid(buf...) {
		return 0, 0, run, xerrors.New("invalid index values found for dictionary converter")
	}

	var (
		read    int
		skipped int
	)
	for read < batch {
		if run.Set {
			updateSize := utils.Min(batch-read, int(run.Len))
			if err := dc.Copy(vals, buf[read:read+updateSize]); err != nil {
				return 0, 0, run, err
			}
			read += updateSize
			vals = vals[updateSize:]
			run.Len -= int64(updateSize)
		} else {
			dc.FillZero(vals[:int(run.Len)])
			vals = vals[int(run.Len):]
			skipped += int(run.Len)
			run.Len = 0
		}
		if run.Len == 0 {
			run = bitRdr.NextRun()
		}
	}
	r.litCount -= int32(batch)
	return read, skipped, run, nil
}

func (r *RleDecoder) GetBatchWithDict(dc DictionaryConverter, vals interface{}) (int, error) {
	switch vals := vals.(type) {
	case []int32:
		return r.GetBatchWithDictInt32(dc, vals)
	case []int64:
		return r.GetBatchWithDictInt64(dc, vals)
	case []float32:
		return r.GetBatchWithDictFloat32(dc, vals)
	case []float64:
		return r.GetBatchWithDictFloat64(dc, vals)
	case []parquet.ByteArray:
		return r.GetBatchWithDictByteArray(dc, vals)
	case []parquet.FixedLenByteArray:
		return r.GetBatchWithDictFixedLenByteArray(dc, vals)
	case []parquet.Int96:
		return r.GetBatchWithDictInt96(dc, vals)
	default:
		return 0, xerrors.New("parquet/rle: GetBatchWithDict invalid type")
	}
}

func (r *RleDecoder) GetBatchWithDictSpaced(dc DictionaryConverter, vals interface{}, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	switch vals := vals.(type) {
	case []int32:
		return r.GetBatchWithDictSpacedInt32(dc, vals, nullCount, validBits, validBitsOffset)
	case []int64:
		return r.GetBatchWithDictSpacedInt64(dc, vals, nullCount, validBits, validBitsOffset)
	case []float32:
		return r.GetBatchWithDictSpacedFloat32(dc, vals, nullCount, validBits, validBitsOffset)
	case []float64:
		return r.GetBatchWithDictSpacedFloat64(dc, vals, nullCount, validBits, validBitsOffset)
	case []parquet.ByteArray:
		return r.GetBatchWithDictSpacedByteArray(dc, vals, nullCount, validBits, validBitsOffset)
	case []parquet.FixedLenByteArray:
		return r.GetBatchWithDictSpacedFixedLenByteArray(dc, vals, nullCount, validBits, validBitsOffset)
	case []parquet.Int96:
		return r.GetBatchWithDictSpacedInt96(dc, vals, nullCount, validBits, validBitsOffset)
	default:
		return 0, xerrors.New("parquet/rle: GetBatchWithDictSpaced invalid type")
	}
}

type RleEncoder struct {
	w *BitWriter

	buffer                 []uint64
	BitWidth               int
	curVal                 uint64
	repCount               int32
	litCount               int32
	literalIndicatorOffset int

	indicatorBuffer [1]byte
}

func NewRleEncoder(w WriterAtWithLen, width int) *RleEncoder {
	return &RleEncoder{
		w:                      NewBitWriter(w),
		buffer:                 make([]uint64, 0, 8),
		BitWidth:               width,
		literalIndicatorOffset: -1,
	}
}

func (r *RleEncoder) Flush() int {
	if r.litCount > 0 || r.repCount > 0 || len(r.buffer) > 0 {
		allRep := r.litCount == 0 && (r.repCount == int32(len(r.buffer)) || len(r.buffer) == 0)
		if r.repCount > 0 && allRep {
			r.flushRepeated()
		} else {
			// buffer the last group of literals to 8 by padding with 0s
			for len(r.buffer) != 0 && len(r.buffer) < 8 {
				r.buffer = append(r.buffer, 0)
			}

			r.litCount += int32(len(r.buffer))
			r.flushLiteral(true)
			r.repCount = 0
		}
	}
	r.w.Flush(false)
	return r.w.Written()
}

func (r *RleEncoder) flushBuffered(done bool) (err error) {
	if r.repCount >= 8 {
		// clear buffered values. they are part of the repeated run now and we
		// don't want to flush them as literals
		r.buffer = r.buffer[:0]
		if r.litCount != 0 {
			// there was  current literal run. all values flushed but need to update the indicator
			err = r.flushLiteral(true)
		}
		return
	}

	r.litCount += int32(len(r.buffer))
	ngroups := r.litCount / 8
	if ngroups+1 >= (1 << 6) {
		// we need to start a new literal run because the indicator byte we've reserved
		// cannot store any more values
		err = r.flushLiteral(true)
	} else {
		err = r.flushLiteral(done)
	}
	r.repCount = 0
	return
}

func (r *RleEncoder) flushLiteral(updateIndicator bool) (err error) {
	if r.literalIndicatorOffset == -1 {
		r.literalIndicatorOffset, err = r.w.SkipBytes(1)
		if err != nil {
			return
		}
	}

	for _, val := range r.buffer {
		if err = r.w.WriteValue(val, uint(r.BitWidth)); err != nil {
			return
		}
	}
	r.buffer = r.buffer[:0]

	if updateIndicator {
		// at this point we need to write the indicator byte for the literal run.
		// we only reserve one byte, to allow for streaming writes of literal values.
		// the logic makes sure we flush literal runs often enough to not overrun the 1 byte.
		ngroups := r.litCount / 8
		r.indicatorBuffer[0] = byte((ngroups << 1) | 1)
		_, err = r.w.WriteAt(r.indicatorBuffer[:], int64(r.literalIndicatorOffset))
		r.literalIndicatorOffset = -1
		r.litCount = 0
	}
	return
}

func (r *RleEncoder) flushRepeated() (ret bool) {
	indicator := r.repCount << 1

	ret = r.w.WriteVlqInt(uint64(indicator))
	ret = ret && r.w.WriteAligned(r.curVal, int(bitutil.BytesForBits(int64(r.BitWidth))))

	r.repCount = 0
	r.buffer = r.buffer[:0]
	return
}

// Put buffers input values 8 at a time. after seeing all 8 values,
// it decides whether they should be encoded as a literal or repeated run.
func (r *RleEncoder) Put(value uint64) error {
	if r.curVal == value {
		r.repCount++
		if r.repCount > 8 {
			// this is just a continuation of the current run, no need to buffer the values
			// NOTE this is the fast path for long repeated runs
			return nil
		}
	} else {
		if r.repCount >= 8 {
			if !r.flushRepeated() {
				return xerrors.New("failed to flush repeated value")
			}
		}
		r.repCount = 1
		r.curVal = value
	}

	r.buffer = append(r.buffer, value)
	if len(r.buffer) == 8 {
		return r.flushBuffered(false)
	}
	return nil
}

func (r *RleEncoder) Clear() {
	r.curVal = 0
	r.repCount = 0
	r.buffer = r.buffer[:0]
	r.litCount = 0
	r.literalIndicatorOffset = -1
	r.w.Clear()
}
