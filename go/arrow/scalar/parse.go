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

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/float16"
	"github.com/apache/arrow/go/arrow/memory"
	"golang.org/x/xerrors"
)

func MakeScalar(val interface{}) (Scalar, error) {
	switch v := val.(type) {
	case nil:
		return ScalarNull, nil
	case bool:
		return NewBooleanScalar(v), nil
	case int8:
		return NewInt8Scalar(v), nil
	case uint8:
		return NewUint8Scalar(v), nil
	case int16:
		return NewInt16Scalar(v), nil
	case uint16:
		return NewUint16Scalar(v), nil
	case int32:
		return NewInt32Scalar(v), nil
	case uint32:
		return NewUint32Scalar(v), nil
	case int64:
		return NewInt64Scalar(v), nil
	case uint64:
		return NewUint64Scalar(v), nil
	case int:
		// determine size of an int on this system
		switch bits.UintSize {
		case 32:
			return NewInt32Scalar(int32(v)), nil
		case 64:
			return NewInt64Scalar(int64(v)), nil
		}
	case uint:
		// determine size of an int on this system
		switch bits.UintSize {
		case 32:
			return NewUint32Scalar(uint32(v)), nil
		case 64:
			return NewUint64Scalar(uint64(v)), nil
		}
	case []byte:
		buf := memory.NewBufferBytes(v)
		defer buf.Release()
		return NewBinaryScalar(buf, arrow.BinaryTypes.Binary), nil
	case string:
		return NewStringScalar(v), nil
	case arrow.Date32:
		return NewDate32Scalar(v), nil
	case arrow.Date64:
		return NewDate64Scalar(v), nil
	case float16.Num:
		return NewFloat16Scalar(v), nil
	case float32:
		return NewFloat32Scalar(v), nil
	case float64:
		return NewFloat64Scalar(v), nil
	case arrow.MonthInterval:
		return NewMonthIntervalScalar(v), nil
	case arrow.DayTimeInterval:
		return NewDayTimeIntervalScalar(v), nil
	}

	return nil, xerrors.Errorf("makescalar not implemented for type value %#v", val)
}

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

func ParseScalar(dt arrow.DataType, val string) (Scalar, error) {
	switch dt.ID() {
	case arrow.STRING:
		return NewStringScalar(val), nil
	case arrow.BINARY:
		buf := memory.NewBufferBytes([]byte(val))
		defer buf.Release()
		return NewBinaryScalar(buf, dt), nil
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
	case arrow.TIMESTAMP:
		format := time.RFC3339
		if dt.(*arrow.TimestampType).Unit == arrow.Microsecond || dt.(*arrow.TimestampType).Unit == arrow.Nanosecond {
			format = time.RFC3339Nano
		}
		out, err := time.ParseInLocation(format, val, time.UTC)
		if err != nil {
			return nil, err
		}

		value := arrow.Timestamp(ConvertTimestampValue(arrow.Nanosecond, dt.(*arrow.TimestampType).Unit, out.UnixNano()))
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
			return NewDate32Scalar(arrow.Date32(out.Sub(time.Unix(0, 0)).Hours() / 24)), nil
		} else {
			return NewDate64Scalar(arrow.Date64(out.Unix() * 1000)), nil
		}
	case arrow.TIME32:
		var (
			out time.Time
			err error
		)
		switch {
		case len(val) == 5:
			out, err = time.ParseInLocation("15:04", val, time.UTC)
		default:
			out, err = time.ParseInLocation("15:04:05,999", val, time.UTC)
		}
		if err != nil {
			return nil, err
		}
		t := out.Sub(time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC))
		if dt.(*arrow.Time32Type).Unit == arrow.Second {
			return NewTime32Scalar(arrow.Time32(t.Seconds()), dt), nil
		}
		return NewTime32Scalar(arrow.Time32(t.Milliseconds()), dt), nil
	case arrow.TIME64:
		var (
			out time.Time
			err error
		)
		switch {
		case len(val) == 5:
			out, err = time.ParseInLocation("15:04", val, time.UTC)
		default:
			out, err = time.ParseInLocation("15:04:05,999999999", val, time.UTC)
		}
		if err != nil {
			return nil, err
		}
		t := out.Sub(time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC))
		if dt.(*arrow.Time64Type).Unit == arrow.Microsecond {
			return NewTime64Scalar(arrow.Time64(t.Microseconds()), dt), nil
		}
		return NewTime64Scalar(arrow.Time64(t.Nanoseconds()), dt), nil
	}

	return nil, xerrors.Errorf("parsing of scalar for type %s not implemented", dt)
}
