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

package compute

import (
	"io"
	"math"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/bitutil"
	"github.com/apache/arrow/go/v10/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v10/arrow/internal/debug"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"golang.org/x/xerrors"
)

type bufferWriteSeeker struct {
	buf *memory.Buffer
	pos int
	mem memory.Allocator
}

func (b *bufferWriteSeeker) Reserve(nbytes int) {
	if b.buf == nil {
		b.buf = memory.NewResizableBuffer(b.mem)
	}
	newCap := int(math.Max(float64(b.buf.Cap()), 256))
	for newCap < b.pos+nbytes {
		newCap = bitutil.NextPowerOf2(newCap)
	}
	b.buf.Reserve(newCap)
}

func (b *bufferWriteSeeker) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	if b.buf == nil {
		b.Reserve(len(p))
	} else if b.pos+len(p) >= b.buf.Cap() {
		b.Reserve(len(p))
	}

	return b.UnsafeWrite(p)
}

func (b *bufferWriteSeeker) UnsafeWrite(p []byte) (n int, err error) {
	n = copy(b.buf.Buf()[b.pos:], p)
	b.pos += len(p)
	if b.pos > b.buf.Len() {
		b.buf.ResizeNoShrink(b.pos)
	}
	return
}

func (b *bufferWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	newpos, offs := 0, int(offset)
	switch whence {
	case io.SeekStart:
		newpos = offs
	case io.SeekCurrent:
		newpos = b.pos + offs
	case io.SeekEnd:
		newpos = b.buf.Len() + offs
	}
	if newpos < 0 {
		return 0, xerrors.New("negative result pos")
	}
	b.pos = newpos
	return int64(newpos), nil
}

// ensureDictionaryDecoded is used by DispatchBest to determine
// the proper types for promotion. Casting is then performed by
// the executor before continuing execution: see the implementation
// of execInternal in exec.go after calling DispatchBest.
//
// That casting is where actual decoding would be performed for
// the dictionary
func ensureDictionaryDecoded(vals ...arrow.DataType) {
	for i, v := range vals {
		if v.ID() == arrow.DICTIONARY {
			vals[i] = v.(*arrow.DictionaryType).ValueType
		}
	}
}

func replaceNullWithOtherType(vals ...arrow.DataType) {
	debug.Assert(len(vals) == 2, "should be length 2")

	if vals[0].ID() == arrow.NULL {
		vals[0] = vals[1]
		return
	}

	if vals[1].ID() == arrow.NULL {
		vals[1] = vals[0]
		return
	}
}

func commonTemporalResolution(vals ...arrow.DataType) (arrow.TimeUnit, bool) {
	isTimeUnit := false
	finestUnit := arrow.Second
	for _, v := range vals {
		switch dt := v.(type) {
		case *arrow.Date32Type:
			isTimeUnit = true
			continue
		case *arrow.Date64Type:
			finestUnit = exec.Max(finestUnit, arrow.Millisecond)
			isTimeUnit = true
		case arrow.TemporalWithUnit:
			finestUnit = exec.Max(finestUnit, dt.TimeUnit())
			isTimeUnit = true
		default:
			continue
		}
	}
	return finestUnit, isTimeUnit
}

func replaceTemporalTypes(unit arrow.TimeUnit, vals ...arrow.DataType) {
	for i, v := range vals {
		switch dt := v.(type) {
		case *arrow.TimestampType:
			dt.Unit = unit
			vals[i] = dt
		case *arrow.Time32Type, *arrow.Time64Type:
			if unit > arrow.Millisecond {
				vals[i] = &arrow.Time64Type{Unit: unit}
			} else {
				vals[i] = &arrow.Time32Type{Unit: unit}
			}
		case *arrow.DurationType:
			dt.Unit = unit
			vals[i] = dt
		case *arrow.Date32Type, *arrow.Date64Type:
			vals[i] = &arrow.TimestampType{Unit: unit}
		}
	}
}

func replaceTypes(replacement arrow.DataType, vals ...arrow.DataType) {
	for i := range vals {
		vals[i] = replacement
	}
}

func commonNumeric(vals ...arrow.DataType) arrow.DataType {
	for _, v := range vals {
		if !arrow.IsFloating(v.ID()) && !arrow.IsInteger(v.ID()) {
			// a common numeric type is only possible if all are numeric
			return nil
		}
		if v.ID() == arrow.FLOAT16 {
			// float16 arithmetic is not currently supported
			return nil
		}
	}

	for _, v := range vals {
		if v.ID() == arrow.FLOAT64 {
			return arrow.PrimitiveTypes.Float64
		}
	}

	for _, v := range vals {
		if v.ID() == arrow.FLOAT32 {
			return arrow.PrimitiveTypes.Float32
		}
	}

	maxWidthSigned, maxWidthUnsigned := 0, 0
	for _, v := range vals {
		if arrow.IsUnsignedInteger(v.ID()) {
			maxWidthUnsigned = exec.Max(v.(arrow.FixedWidthDataType).BitWidth(), maxWidthUnsigned)
		} else {
			maxWidthSigned = exec.Max(v.(arrow.FixedWidthDataType).BitWidth(), maxWidthSigned)
		}
	}

	if maxWidthSigned == 0 {
		switch {
		case maxWidthUnsigned >= 64:
			return arrow.PrimitiveTypes.Uint64
		case maxWidthUnsigned == 32:
			return arrow.PrimitiveTypes.Uint32
		case maxWidthUnsigned == 16:
			return arrow.PrimitiveTypes.Uint16
		default:
			debug.Assert(maxWidthUnsigned == 8, "bad maxWidthUnsigned")
			return arrow.PrimitiveTypes.Uint8
		}
	}

	if maxWidthSigned <= maxWidthUnsigned {
		maxWidthSigned = bitutil.NextPowerOf2(maxWidthUnsigned + 1)
	}

	switch {
	case maxWidthSigned >= 64:
		return arrow.PrimitiveTypes.Int64
	case maxWidthSigned == 32:
		return arrow.PrimitiveTypes.Int32
	case maxWidthSigned == 16:
		return arrow.PrimitiveTypes.Int16
	default:
		debug.Assert(maxWidthSigned == 8, "bad maxWidthSigned")
		return arrow.PrimitiveTypes.Int8
	}
}

func hasDecimal(vals ...arrow.DataType) bool {
	for _, v := range vals {
		if arrow.IsDecimal(v.ID()) {
			return true
		}
	}

	return false
}

type decimalPromotion uint8

const (
	decPromoteNone decimalPromotion = iota
	decPromoteAdd
	decPromoteMultiply
	decPromoteDivide
)

func castBinaryDecimalArgs(promote decimalPromotion, vals ...arrow.DataType) error {
	return arrow.ErrNotImplemented
}
