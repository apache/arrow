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

package bitutils_test

import (
	"testing"

	"github.com/apache/arrow/go/v12/arrow/bitutil"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/internal/bitutils"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/rand"
)

const kWordSize = 64

func create(nbytes, offset, length int64) (*memory.Buffer, *bitutils.BitBlockCounter) {
	buf := memory.NewResizableBuffer(memory.DefaultAllocator)
	buf.Resize(int(nbytes))
	return buf, bitutils.NewBitBlockCounter(buf.Bytes(), offset, length)
}

func TestOneWordBasics(t *testing.T) {
	const nbytes = 1024

	buf, counter := create(nbytes, 0, nbytes*8)
	defer buf.Release()

	var bitsScanned int64
	for i := 0; i < nbytes/8; i++ {
		block := counter.NextWord()
		assert.EqualValues(t, kWordSize, block.Len)
		assert.EqualValues(t, 0, block.Popcnt)
		bitsScanned += int64(block.Len)
	}
	assert.EqualValues(t, 1024*8, bitsScanned)

	block := counter.NextWord()
	assert.Zero(t, block.Len)
	assert.Zero(t, block.Popcnt)
	assert.True(t, block.NoneSet())
}

func TestFourWordsBasics(t *testing.T) {
	const nbytes = 1024

	buf, counter := create(nbytes, 0, nbytes*8)
	defer buf.Release()

	var bitsScanned int64
	for i := 0; i < nbytes/32; i++ {
		block := counter.NextFourWords()
		assert.EqualValues(t, 4*kWordSize, block.Len)
		assert.EqualValues(t, 0, block.Popcnt)
		bitsScanned += int64(block.Len)
	}
	assert.EqualValues(t, 1024*8, bitsScanned)

	block := counter.NextFourWords()
	assert.Zero(t, block.Len)
	assert.Zero(t, block.Popcnt)
}

func TestOneWordWithOffsets(t *testing.T) {
	checkWithOffset := func(offset int64) {
		const (
			nwords     int64 = 4
			totalBytes       = nwords*8 + 1
		)

		// Trim a bit from the end of the bitmap so we can check
		// the remainder bits behavior
		buf, counter := create(totalBytes, offset, nwords*kWordSize-offset-1)
		defer buf.Release()

		memory.Set(buf.Bytes(), byte(0xFF))

		block := counter.NextWord()
		assert.EqualValues(t, kWordSize, block.Len)
		assert.EqualValues(t, 64, block.Popcnt)

		// add a false value to the next word
		bitutil.SetBitTo(buf.Bytes(), kWordSize+int(offset), false)
		block = counter.NextWord()
		assert.EqualValues(t, 64, block.Len)
		assert.EqualValues(t, 63, block.Popcnt)

		// Set the next word to all false
		bitutil.SetBitsTo(buf.Bytes(), 2*kWordSize+offset, kWordSize, false)

		block = counter.NextWord()
		assert.EqualValues(t, 64, block.Len)
		assert.Zero(t, block.Popcnt)

		block = counter.NextWord()
		assert.EqualValues(t, kWordSize-offset-1, block.Len)
		assert.EqualValues(t, block.Len, block.Popcnt)
		assert.True(t, block.AllSet())

		// we can keep calling nextword safely
		block = counter.NextWord()
		assert.Zero(t, block.Len)
		assert.Zero(t, block.Popcnt)
	}

	for offsetI := int64(0); offsetI < 8; offsetI++ {
		checkWithOffset(offsetI)
	}
}

func TestFourWordsWithOffsets(t *testing.T) {
	checkWithOffset := func(offset int64) {
		const (
			nwords     = 17
			totalBytes = nwords*8 + 1
		)

		// trim a bit from the end of the bitmap so we can check the remainder
		// bits behavior
		buf, counter := create(totalBytes, offset, nwords*kWordSize-offset-1)

		// start with all set
		memory.Set(buf.Bytes(), 0xFF)

		block := counter.NextFourWords()
		assert.EqualValues(t, 4*kWordSize, block.Len)
		assert.EqualValues(t, block.Len, block.Popcnt)

		// add some false values to the next 3 shifted words
		bitutil.ClearBit(buf.Bytes(), int(4*kWordSize+offset))
		bitutil.ClearBit(buf.Bytes(), int(5*kWordSize+offset))
		bitutil.ClearBit(buf.Bytes(), int(6*kWordSize+offset))

		block = counter.NextFourWords()
		assert.EqualValues(t, 4*kWordSize, block.Len)
		assert.EqualValues(t, 253, block.Popcnt)

		// set the next two words to all false
		bitutil.SetBitsTo(buf.Bytes(), 8*kWordSize+offset, 2*kWordSize, false)

		// block is half set
		block = counter.NextFourWords()
		assert.EqualValues(t, 4*kWordSize, block.Len)
		assert.EqualValues(t, 128, block.Popcnt)

		// last full block whether offset or no
		block = counter.NextFourWords()
		assert.EqualValues(t, 4*kWordSize, block.Len)
		assert.EqualValues(t, block.Len, block.Popcnt)

		// partial block
		block = counter.NextFourWords()
		assert.EqualValues(t, kWordSize-offset-1, block.Len)
		assert.EqualValues(t, block.Len, block.Popcnt)

		// we can keep calling NextFourWords safely
		block = counter.NextFourWords()
		assert.Zero(t, block.Len)
		assert.Zero(t, block.Popcnt)
	}

	for offsetI := int64(0); offsetI < 8; offsetI++ {
		checkWithOffset(offsetI)
	}
}

func TestFourWordsRandomData(t *testing.T) {
	const (
		nbytes = 1024
	)

	buf := make([]byte, nbytes)
	r := rand.New(rand.NewSource(0))
	r.Read(buf)

	checkWithOffset := func(offset int64) {
		counter := bitutils.NewBitBlockCounter(buf, offset, nbytes*8-offset)
		for i := 0; i < nbytes/32; i++ {
			block := counter.NextFourWords()
			assert.EqualValues(t, bitutil.CountSetBits(buf, i*256+int(offset), int(block.Len)), block.Popcnt)
		}
	}

	for offsetI := int64(0); offsetI < 8; offsetI++ {
		checkWithOffset(offsetI)
	}
}
