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

package scalar

import (
	"math/bits"
	"strconv"
	"time"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/float16"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"golang.org/x/xerrors"
)

// MakeScalarParam is for converting a value to a scalar when it requires a
// parameterized data type such as a time type that needs units, or a fixed
// size list which needs it's size.
//
// Will fall back to MakeScalar without the passed in type if not one of the
// parameterized types.
func MakeScalarParam(val interface{}, dt arrow.DataType) (Scalar, error) {
	switch v := val.(type) {
	case []byte:
		buf := memory.NewBufferBytes(v)
		defer buf.Release()

		switch dt.ID() {
		case arrow.BINARY:
			return NewBinaryScalar(buf, dt), nil
		case arrow.STRING:
			return NewStringScalarFromBuffer(buf), nil
		case arrow.FIXED_SIZE_BINARY:
			if buf.Len() == dt.(*arrow.FixedSizeBinaryType).ByteWidth {
				return NewFixedSizeBinaryScalar(buf, dt), nil
			}
			return nil, xerrors.Errorf("invalid scalar value of len %d for type %s", v, dt)
		}
	case *memory.Buffer:
		switch dt.ID() {
		case arrow.BINARY:
			return NewBinaryScalar(v, dt), nil
		case arrow.STRING:
			return NewStringScalarFromBuffer(v), nil
		case arrow.FIXED_SIZE_BINARY:
			if v.Len() == dt.(*arrow.FixedSizeBinaryType).ByteWidth {
				return NewFixedSizeBinaryScalar(v, dt), nil
			}
			return nil, xerrors.Errorf("invalid scalar value of len %d for type %s", v.Len(), dt)
		}
	case arrow.Time32:
		return NewTime32Scalar(v, dt), nil
	case arrow.Time64:
		return NewTime64Scalar(v, dt), nil
	case arrow.Timestamp:
		return NewTimestampScalar(v, dt), nil
	case array.Interface:
		switch dt.ID() {
		case arrow.LIST:
			if !arrow.TypeEqual(v.DataType(), dt.(*arrow.ListType).Elem()) {
				return nil, xerrors.Errorf("inconsistent type for list scalar array and data type")
			}
			return NewListScalar(v), nil
		case arrow.FIXED_SIZE_LIST:
			if !arrow.TypeEqual(v.DataType(), dt.(*arrow.FixedSizeListType).Elem()) {
				return nil, xerrors.Errorf("inconsistent type for list scalar array and data type")
			}
			return NewFixedSizeListScalarWithType(v, dt), nil
		case arrow.MAP:
			if !arrow.TypeEqual(dt.(*arrow.MapType).ValueType(), v.DataType()) {
				return nil, xerrors.Errorf("inconsistent type for map scalar type")
			}
			return NewMapScalar(v), nil
		}
	}
	return MakeScalar(val), nil
}

// MakeScalar creates a scalar of the passed in type via reflection.
func MakeScalar(val interface{}) Scalar {
	switch v := val.(type) {
	case nil:
		return ScalarNull
	case bool:
		return NewBooleanScalar(v)
	case int8:
		return NewInt8Scalar(v)
	case uint8:
		return NewUint8Scalar(v)
	case int16:
		return NewInt16Scalar(v)
	case uint16:
		return NewUint16Scalar(v)
	case int32:
		return NewInt32Scalar(v)
	case uint32:
		return NewUint32Scalar(v)
	case int64:
		return NewInt64Scalar(v)
	case uint64:
		return NewUint64Scalar(v)
	case int:
		// determine size of an int on this system
		switch bits.UintSize {
		case 32:
			return NewInt32Scalar(int32(v))
		case 64:
			return NewInt64Scalar(int64(v))
		}
	case uint:
		// determine size of an int on this system
		switch bits.UintSize {
		case 32:
			return NewUint32Scalar(uint32(v))
		case 64:
			return NewUint64Scalar(uint64(v))
		}
	case []byte:
		buf := memory.NewBufferBytes(v)
		defer buf.Release()
		return NewBinaryScalar(buf, arrow.BinaryTypes.Binary)
	case string:
		return NewStringScalar(v)
	case arrow.Date32:
		return NewDate32Scalar(v)
	case arrow.Date64:
		return NewDate64Scalar(v)
	case float16.Num:
		return NewFloat16Scalar(v)
	case float32:
		return NewFloat32Scalar(v)
	case float64:
		return NewFloat64Scalar(v)
	case arrow.MonthInterval:
		return NewMonthIntervalScalar(v)
	case arrow.DayTimeInterval:
		return NewDayTimeIntervalScalar(v)
	case arrow.MonthDayNanoInterval:
		return NewMonthDayNanoIntervalScalar(v)
	case arrow.DataType:
		return MakeNullScalar(v)
	}

	panic(xerrors.Errorf("makescalar not implemented for type value %#v", val))
}

// MakeIntegerScalar is a helper function for creating an integer scalar of a
// given bitsize.
func MakeIntegerScalar(v int64, bitsize int) (Scalar, error) {
	switch bitsize {
	case 8:
		return NewInt8Scalar(int8(v)), nil
	case 16:
		return NewInt16Scalar(int16(v)), nil
	case 32:
		return NewInt32Scalar(int32(v)), nil
	case 64:
		return NewInt64Scalar(int64(v)), nil
	}
	return nil, xerrors.Errorf("invalid bitsize for integer scalar: %d", bitsize)
}

// MakeUnsignedIntegerScalar is a helper function for creating an unsigned int
// scalar of the specified bit width.
func MakeUnsignedIntegerScalar(v uint64, bitsize int) (Scalar, error) {
	switch bitsize {
	case 8:
		return NewUint8Scalar(uint8(v)), nil
	case 16:
		return NewUint16Scalar(uint16(v)), nil
	case 32:
		return NewUint32Scalar(uint32(v)), nil
	case 64:
		return NewUint64Scalar(uint64(v)), nil
	}
	return nil, xerrors.Errorf("invalid bitsize for uint scalar: %d", bitsize)
}

// ParseScalar parses a string to create a scalar of the passed in type. Currently
// does not support any nested types such as Structs or Lists.
func ParseScalar(dt arrow.DataType, val string) (Scalar, error) {
	switch dt.ID() {
	case arrow.STRING:
		return NewStringScalar(val), nil
	case arrow.BINARY:
		buf := memory.NewBufferBytes([]byte(val))
		defer buf.Release()
		return NewBinaryScalar(buf, dt), nil
	case arrow.FIXED_SIZE_BINARY:
		if len(val) != dt.(*arrow.FixedSizeBinaryType).ByteWidth {
			return nil, xerrors.Errorf("invalid value %s for scalar of type %s", val, dt)
		}
		buf := memory.NewBufferBytes([]byte(val))
		defer buf.Release()
		return NewFixedSizeBinaryScalar(buf, dt), nil
	case arrow.BOOL:
		val, err := strconv.ParseBool(val)
		if err != nil {
			return nil, err
		}
		return NewBooleanScalar(val), nil
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		width := dt.(arrow.FixedWidthDataType).BitWidth()
		val, err := strconv.ParseInt(val, 0, width)
		if err != nil {
			return nil, err
		}
		return MakeIntegerScalar(val, width)
	case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		width := dt.(arrow.FixedWidthDataType).BitWidth()
		val, err := strconv.ParseUint(val, 0, width)
		if err != nil {
			return nil, err
		}
		return MakeUnsignedIntegerScalar(val, width)
	case arrow.FLOAT16:
		val, err := strconv.ParseFloat(val, 32)
		if err != nil {
			return nil, err
		}
		return NewFloat16ScalarFromFloat32(float32(val)), nil
	case arrow.FLOAT32, arrow.FLOAT64:
		width := dt.(arrow.FixedWidthDataType).BitWidth()
		val, err := strconv.ParseFloat(val, width)
		if err != nil {
			return nil, err
		}
		switch width {
		case 32:
			return NewFloat32Scalar(float32(val)), nil
		case 64:
			return NewFloat64Scalar(float64(val)), nil
		}
	case arrow.TIMESTAMP:
		value, err := arrow.TimestampFromString(val, dt.(*arrow.TimestampType).Unit)
		if err != nil {
			return nil, err
		}
		return NewTimestampScalar(value, dt), nil
	case arrow.DURATION:
		value, err := time.ParseDuration(val)
		if err != nil {
			return nil, err
		}
		unit := dt.(*arrow.DurationType).Unit
		var out arrow.Duration
		switch unit {
		case arrow.Nanosecond:
			out = arrow.Duration(value.Nanoseconds())
		case arrow.Microsecond:
			out = arrow.Duration(value.Microseconds())
		case arrow.Millisecond:
			out = arrow.Duration(value.Milliseconds())
		case arrow.Second:
			out = arrow.Duration(value.Seconds())
		}
		return NewDurationScalar(out, dt), nil
	case arrow.DATE32, arrow.DATE64:
		out, err := time.ParseInLocation("2006-01-02", val, time.UTC)
		if err != nil {
			return nil, err
		}
		if dt.ID() == arrow.DATE32 {
			return NewDate32Scalar(arrow.Date32FromTime(out)), nil
		} else {
			return NewDate64Scalar(arrow.Date64FromTime(out)), nil
		}
	case arrow.TIME32:
		tm, err := arrow.Time32FromString(val, dt.(*arrow.Time32Type).Unit)
		if err != nil {
			return nil, err
		}

		return NewTime32Scalar(tm, dt), nil
	case arrow.TIME64:
		tm, err := arrow.Time64FromString(val, dt.(*arrow.Time64Type).Unit)
		if err != nil {
			return nil, err
		}

		return NewTime64Scalar(tm, dt), nil
	}

	return nil, xerrors.Errorf("parsing of scalar for type %s not implemented", dt)
}
