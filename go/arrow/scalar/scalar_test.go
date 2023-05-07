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

package scalar_test

import (
	"bytes"
	"fmt"
	"hash/maphash"
	"math/bits"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/decimal128"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/arrow/scalar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func assertScalarsEqual(t *testing.T, expected, actual scalar.Scalar) {
	assert.Truef(t, scalar.Equals(expected, actual), "Expected:\n%s\nActual:\n%s", expected, actual)
	seed := maphash.MakeSeed()
	assert.Equal(t, scalar.Hash(seed, expected), scalar.Hash(seed, actual))
}

func assertMakeScalarParam(t *testing.T, expected scalar.Scalar, dt arrow.DataType, val interface{}) {
	out, err := scalar.MakeScalarParam(val, dt)
	assert.NoError(t, err)
	assert.NoError(t, out.Validate())
	assert.NoError(t, out.ValidateFull())
	assertScalarsEqual(t, expected, out)
}

func assertMakeScalar(t *testing.T, expected scalar.Scalar, val interface{}) {
	out := scalar.MakeScalar(val)
	assert.NoError(t, out.Validate())
	assert.NoError(t, out.ValidateFull())
	assertScalarsEqual(t, expected, out)
}

func assertParseScalar(t *testing.T, dt arrow.DataType, str string, expected scalar.Scalar) {
	out, err := scalar.ParseScalar(dt, str)
	assert.NoError(t, err)
	assert.NoError(t, out.Validate())
	assert.NoError(t, out.ValidateFull())
	assertScalarsEqual(t, expected, out)
}

func TestMakeScalarInt(t *testing.T) {
	three := scalar.MakeScalar(int(3))
	assert.NoError(t, three.ValidateFull())

	var expected scalar.Scalar
	if bits.UintSize == 32 {
		expected = scalar.NewInt32Scalar(3)
	} else {
		expected = scalar.NewInt64Scalar(3)
	}

	assert.Equal(t, expected, three)
	assertMakeScalar(t, expected, int(3))
	assertParseScalar(t, expected.DataType(), "3", expected)
}

func checkMakeNullScalar(t *testing.T, dt arrow.DataType) scalar.Scalar {
	s := scalar.MakeNullScalar(dt)
	assert.NoError(t, s.Validate())
	assert.NoError(t, s.ValidateFull())
	assert.True(t, arrow.TypeEqual(s.DataType(), dt))
	assert.False(t, s.IsValid())
	return s
}

func TestMakeScalarUint(t *testing.T) {
	three := scalar.MakeScalar(uint(3))
	assert.NoError(t, three.ValidateFull())

	var expected scalar.Scalar
	if bits.UintSize == 32 {
		expected = scalar.NewUint32Scalar(3)
	} else {
		expected = scalar.NewUint64Scalar(3)
	}

	assert.Equal(t, expected, three)
	assertMakeScalar(t, expected, uint(3))
	assertParseScalar(t, expected.DataType(), "3", expected)
}

func TestBasicDecimal128(t *testing.T) {
	ty := &arrow.Decimal128Type{Precision: 3, Scale: 2}
	pi := scalar.NewDecimal128Scalar(decimal128.New(0, 314), ty)
	pi2 := scalar.NewDecimal128Scalar(decimal128.FromI64(628), ty)
	null := checkMakeNullScalar(t, ty)

	assert.NoError(t, pi.ValidateFull())
	assert.True(t, pi.IsValid())
	assert.Equal(t, decimal128.FromI64(314), pi.Value)

	assert.NoError(t, null.ValidateFull())
	assert.False(t, null.IsValid())

	assert.False(t, scalar.Equals(pi, pi2))
}

func TestBinaryScalarBasics(t *testing.T) {
	data := "test data"
	buf := memory.NewBufferBytes([]byte(data))

	value := scalar.NewBinaryScalar(buf, arrow.BinaryTypes.Binary)
	assert.NoError(t, value.ValidateFull())
	assert.True(t, bytes.Equal(value.Value.Bytes(), buf.Bytes()))
	assert.True(t, value.IsValid())
	assert.True(t, arrow.TypeEqual(value.DataType(), arrow.BinaryTypes.Binary))

	nullValue := checkMakeNullScalar(t, arrow.BinaryTypes.Binary)
	assert.False(t, nullValue.IsValid())
	assert.Nil(t, nullValue.(*scalar.Binary).Value)
	assert.NoError(t, nullValue.ValidateFull())

	value2 := scalar.NewStringScalarFromBuffer(buf)
	assert.NoError(t, value2.ValidateFull())
	assert.True(t, bytes.Equal(value2.Value.Bytes(), buf.Bytes()))
	assert.True(t, value2.IsValid())
	assert.True(t, arrow.TypeEqual(arrow.BinaryTypes.String, value2.DataType()))

	assert.NotEqual(t, value2, value)
	assert.False(t, scalar.Equals(value2, value))

	value3 := scalar.NewStringScalar(data)
	assert.True(t, scalar.Equals(value2, value3))
}

func TestBinaryScalarValidateErrors(t *testing.T) {
	sc := scalar.NewBinaryScalar(memory.NewBufferBytes([]byte("xxx")), arrow.BinaryTypes.Binary)
	sc.Valid = false
	assert.Error(t, sc.Validate())
	assert.Error(t, sc.ValidateFull())

	nullScalar := scalar.MakeNullScalar(arrow.BinaryTypes.Binary)
	nullScalar.(*scalar.Binary).Valid = true
	assert.Error(t, sc.Validate())
	assert.Error(t, sc.ValidateFull())
}

func TestStringMakeScalar(t *testing.T) {
	assertMakeScalar(t, scalar.NewStringScalar("three"), "three")
	assertParseScalar(t, arrow.BinaryTypes.String, "three", scalar.NewStringScalar("three"))
}

func TestStringScalarValidateErrors(t *testing.T) {
	sc := scalar.NewStringScalar("xxx")
	sc.Valid = false
	assert.Error(t, sc.Validate())
	assert.Error(t, sc.ValidateFull())

	nullScalar := scalar.MakeNullScalar(arrow.BinaryTypes.String)
	nullScalar.(*scalar.String).Valid = true
	assert.Error(t, sc.Validate())
	assert.Error(t, sc.ValidateFull())

	// invalid utf8
	sc = scalar.NewStringScalarFromBuffer(memory.NewBufferBytes([]byte{0xff}))
	assert.NoError(t, sc.Validate())
	assert.Error(t, sc.ValidateFull())
}

func TestFixedSizeBinaryScalarBasics(t *testing.T) {
	data := "test data"
	buf := memory.NewBufferBytes([]byte(data))

	exType := &arrow.FixedSizeBinaryType{ByteWidth: 9}

	value := scalar.NewFixedSizeBinaryScalar(buf, exType)
	assert.NoError(t, value.ValidateFull())
	assert.True(t, bytes.Equal(value.Value.Bytes(), buf.Bytes()))
	assert.True(t, value.Valid)
	assert.True(t, arrow.TypeEqual(value.DataType(), exType))

	nullValue := scalar.MakeNullScalar(exType)
	assert.NoError(t, nullValue.ValidateFull())
	assert.False(t, nullValue.IsValid())
	assert.Nil(t, nullValue.(*scalar.FixedSizeBinary).Value)
}

func TestFixedSizeBinaryMakeScalar(t *testing.T) {
	data := "test data"
	buf := memory.NewBufferBytes([]byte(data))
	exType := &arrow.FixedSizeBinaryType{ByteWidth: 9}

	assertMakeScalarParam(t, scalar.NewFixedSizeBinaryScalar(buf, exType), exType, buf)
	assertParseScalar(t, exType, data, scalar.NewFixedSizeBinaryScalar(buf, exType))

	_, err := scalar.MakeScalarParam(buf.Bytes()[:3], exType)
	assert.Error(t, err)
	_, err = scalar.ParseScalar(exType, data[:3])
	assert.Error(t, err)
}

func TestFixedSizeBinaryScalarValidateErrors(t *testing.T) {
	data := "test data"
	buf := memory.NewBufferBytes([]byte(data))
	exType := &arrow.FixedSizeBinaryType{ByteWidth: 9}

	value := scalar.NewFixedSizeBinaryScalar(buf, exType)
	assert.NoError(t, value.ValidateFull())

	value.Value.Reset(buf.Bytes()[:1])
	assert.Error(t, value.ValidateFull())
}

func TestDateScalarBasics(t *testing.T) {
	i32Val := arrow.Date32(1)
	date32Val := scalar.NewDate32Scalar(i32Val)
	date32Null := scalar.MakeNullScalar(arrow.FixedWidthTypes.Date32)
	assert.NoError(t, date32Null.ValidateFull())
	assert.NoError(t, date32Val.ValidateFull())

	assert.True(t, arrow.TypeEqual(arrow.FixedWidthTypes.Date32, date32Val.DataType()))
	assert.True(t, date32Val.IsValid())
	assert.False(t, date32Null.IsValid())

	i64Val := arrow.Date64(2)
	date64Val := scalar.NewDate64Scalar(i64Val)
	date64Null := scalar.MakeNullScalar(arrow.FixedWidthTypes.Date64)
	assert.NoError(t, date64Null.ValidateFull())
	assert.NoError(t, date64Val.ValidateFull())

	assert.True(t, arrow.TypeEqual(arrow.FixedWidthTypes.Date64, date64Val.DataType()))
	assert.True(t, date64Val.IsValid())
	assert.False(t, date64Null.IsValid())
}

func TestDateScalarMakeScalar(t *testing.T) {
	assertMakeScalar(t, scalar.NewDate32Scalar(arrow.Date32(1)), arrow.Date32(1))
	assertParseScalar(t, arrow.FixedWidthTypes.Date32, "1454-10-22", scalar.NewDate32Scalar(arrow.Date32(-188171)))
	assert.Equal(t, "1454-10-22", scalar.NewDate32Scalar(arrow.Date32(-188171)).String())

	assertMakeScalar(t, scalar.NewDate64Scalar(arrow.Date64(1)), arrow.Date64(1))
	assertParseScalar(t, arrow.FixedWidthTypes.Date64, "1454-10-22", scalar.NewDate64Scalar(arrow.Date64(-188171*(time.Hour*24).Milliseconds())))
	assert.Equal(t, "1454-10-22", scalar.NewDate64Scalar(arrow.Date64(-188171*(time.Hour*24).Milliseconds())).String())

	d32 := scalar.NewDate32Scalar(arrow.Date32(-188171))
	d64 := scalar.NewDate64Scalar(arrow.Date64(-188171 * (time.Hour * 24).Milliseconds()))

	d32Casted, err := d32.CastTo(arrow.FixedWidthTypes.Date64)
	assert.NoError(t, err)
	assert.True(t, scalar.Equals(d64, d32Casted))

	d64Casted, err := d64.CastTo(arrow.FixedWidthTypes.Date32)
	assert.NoError(t, err)
	assert.True(t, scalar.Equals(d64Casted, d32))
}

func TestTimeScalarsBasics(t *testing.T) {
	typ1 := arrow.FixedWidthTypes.Time32ms
	typ2 := arrow.FixedWidthTypes.Time32s
	typ3 := arrow.FixedWidthTypes.Time64us
	typ4 := arrow.FixedWidthTypes.Time64ns

	t32val := arrow.Time32(1)
	time32Val := scalar.NewTime32Scalar(t32val, typ1)
	time32Null := scalar.MakeNullScalar(typ2)
	assert.NoError(t, time32Val.ValidateFull())
	assert.NoError(t, time32Null.ValidateFull())

	assert.Equal(t, t32val, time32Val.Value)
	assert.True(t, arrow.TypeEqual(time32Val.Type, typ1))
	assert.True(t, time32Val.IsValid())
	assert.False(t, time32Null.IsValid())
	assert.True(t, arrow.TypeEqual(time32Null.DataType(), typ2))

	t64val := arrow.Time64(1)
	time64Val := scalar.NewTime64Scalar(t64val, typ3)
	time64Null := scalar.MakeNullScalar(typ4)
	assert.NoError(t, time64Val.ValidateFull())
	assert.NoError(t, time64Null.ValidateFull())

	assert.Equal(t, t64val, time64Val.Value)
	assert.True(t, arrow.TypeEqual(time64Val.Type, typ3))
	assert.True(t, time64Val.IsValid())
	assert.False(t, time64Null.IsValid())
	assert.True(t, arrow.TypeEqual(time64Null.DataType(), typ4))
}

func TestTimeScalarsMakeScalar(t *testing.T) {
	typ1 := arrow.FixedWidthTypes.Time32s
	typ2 := arrow.FixedWidthTypes.Time32ms
	typ3 := arrow.FixedWidthTypes.Time64us
	typ4 := arrow.FixedWidthTypes.Time64ns

	assertMakeScalarParam(t, scalar.NewTime32Scalar(arrow.Time32(1), typ1), typ1, arrow.Time32(1))
	assertMakeScalarParam(t, scalar.NewTime32Scalar(arrow.Time32(1), typ2), typ2, arrow.Time32(1))
	assertMakeScalarParam(t, scalar.NewTime64Scalar(arrow.Time64(1), typ3), typ3, arrow.Time64(1))
	assertMakeScalarParam(t, scalar.NewTime64Scalar(arrow.Time64(1), typ4), typ4, arrow.Time64(1))

	tententen := 60*(60*(10)+10) + 10
	assertParseScalar(t, typ1, "10:10:10", scalar.NewTime32Scalar(arrow.Time32(tententen), typ1))
	assert.Equal(t, "10:10:10", scalar.NewTime32Scalar(arrow.Time32(tententen), typ1).String())

	tententen = 1000*tententen + 123
	assertParseScalar(t, typ2, "10:10:10.123", scalar.NewTime32Scalar(arrow.Time32(tententen), typ2))
	assert.Equal(t, "10:10:10.123", scalar.NewTime32Scalar(arrow.Time32(tententen), typ2).String())

	tententen = 1000*tententen + 456
	assertParseScalar(t, typ3, "10:10:10.123456", scalar.NewTime64Scalar(arrow.Time64(tententen), typ3))
	assert.Equal(t, "10:10:10.123456", scalar.NewTime64Scalar(arrow.Time64(tententen), typ3).String())

	tententen = 1000*tententen + 789
	assertParseScalar(t, typ4, "10:10:10.123456789", scalar.NewTime64Scalar(arrow.Time64(tententen), typ4))
	assert.Equal(t, "10:10:10.123456789", scalar.NewTime64Scalar(arrow.Time64(tententen), typ4).String())
}

func TestTimestampScalarBasics(t *testing.T) {
	typ1 := arrow.FixedWidthTypes.Timestamp_ms
	typ2 := arrow.FixedWidthTypes.Timestamp_s

	val1 := arrow.Timestamp(1)
	val2 := arrow.Timestamp(2)
	tsVal1 := scalar.NewTimestampScalar(val1, typ1)
	tsVal2 := scalar.NewTimestampScalar(val2, typ2)
	tsNull := scalar.MakeNullScalar(typ1)
	assert.NoError(t, tsVal1.ValidateFull())
	assert.NoError(t, tsVal2.ValidateFull())
	assert.NoError(t, tsNull.ValidateFull())

	assert.Equal(t, val1, tsVal1.Value)

	assert.True(t, arrow.TypeEqual(tsVal1.Type, typ1))
	assert.True(t, arrow.TypeEqual(tsVal2.DataType(), typ2))
	assert.True(t, tsVal1.Valid)
	assert.True(t, tsVal2.IsValid())
	assert.False(t, tsNull.IsValid())
	assert.True(t, arrow.TypeEqual(tsNull.DataType(), typ1))

	assert.NotEqual(t, tsVal1, tsVal2)
	assert.False(t, scalar.Equals(tsVal1, tsVal2))
	assert.NotEqual(t, tsVal1, tsNull)
	assert.False(t, scalar.Equals(tsVal1, tsNull))
	assert.NotEqual(t, tsVal2, tsNull)
	assert.False(t, scalar.Equals(tsVal2, tsNull))
}

func TestTimestampScalarsMakeScalar(t *testing.T) {
	typ1 := arrow.FixedWidthTypes.Timestamp_ms
	typ2 := arrow.FixedWidthTypes.Timestamp_s
	typ3 := arrow.FixedWidthTypes.Timestamp_us
	typ4 := arrow.FixedWidthTypes.Timestamp_ns

	epochPlus1s := "1970-01-01 00:00:01"

	assertMakeScalarParam(t, scalar.NewTimestampScalar(arrow.Timestamp(1), typ1), typ1, arrow.Timestamp(1))
	assertParseScalar(t, typ1, epochPlus1s, scalar.NewTimestampScalar(1000, typ1))

	assertMakeScalarParam(t, scalar.NewTimestampScalar(arrow.Timestamp(1), typ2), typ2, arrow.Timestamp(1))
	assertParseScalar(t, typ2, epochPlus1s, scalar.NewTimestampScalar(arrow.Timestamp(1), typ2))

	assertMakeScalarParam(t, scalar.NewTimestampScalar(arrow.Timestamp(1), typ3), typ3, arrow.Timestamp(1))
	assertParseScalar(t, typ3, epochPlus1s, scalar.NewTimestampScalar(arrow.Timestamp(1000*1000), typ3))

	assertMakeScalarParam(t, scalar.NewTimestampScalar(arrow.Timestamp(1), typ4), typ4, arrow.Timestamp(1))
	assertParseScalar(t, typ4, epochPlus1s, scalar.NewTimestampScalar(arrow.Timestamp(1000*1000*1000), typ4))
}

func TestTimestampScalarsCasting(t *testing.T) {
	convert := func(in, out arrow.TimeUnit, val arrow.Timestamp) arrow.Timestamp {
		s, err := scalar.NewTimestampScalar(val, &arrow.TimestampType{Unit: in}).CastTo(&arrow.TimestampType{Unit: out})
		assert.NoError(t, err)
		return s.(*scalar.Timestamp).Value
	}

	assert.EqualValues(t, convert(arrow.Second, arrow.Millisecond, arrow.Timestamp(1)), 1000)
	assert.EqualValues(t, convert(arrow.Second, arrow.Nanosecond, arrow.Timestamp(1)), 1000000000)

	assert.EqualValues(t, convert(arrow.Nanosecond, arrow.Microsecond, arrow.Timestamp(1234)), 1)
	assert.EqualValues(t, convert(arrow.Microsecond, arrow.Millisecond, arrow.Timestamp(4567)), 4)

	str, err := scalar.NewTimestampScalar(arrow.Timestamp(1024), arrow.FixedWidthTypes.Timestamp_ms).CastTo(arrow.BinaryTypes.String)
	assert.NoError(t, err)
	assert.Truef(t, scalar.Equals(scalar.NewStringScalar("1970-01-01 00:00:01.024"), str), "expected: '1970-01-01 00:00:01.024', got: %s", str)

	i64, err := scalar.NewTimestampScalar(arrow.Timestamp(1024), arrow.FixedWidthTypes.Timestamp_ms).CastTo(arrow.PrimitiveTypes.Int64)
	assert.NoError(t, err)
	assert.Truef(t, scalar.Equals(scalar.NewInt64Scalar(1024), i64), "expected 1024, got %s", i64)

	const millisInDay = 86400000
	d64, err := scalar.NewTimestampScalar(arrow.Timestamp(1024*millisInDay+3), arrow.FixedWidthTypes.Timestamp_ms).CastTo(arrow.FixedWidthTypes.Date64)
	assert.NoError(t, err)

	d32, err := scalar.NewTimestampScalar(arrow.Timestamp(1024*millisInDay+3), arrow.FixedWidthTypes.Timestamp_ms).CastTo(arrow.FixedWidthTypes.Date32)
	assert.NoError(t, err)

	assert.True(t, scalar.Equals(scalar.NewDate32Scalar(arrow.Date32(1024)), d32))
	assert.Truef(t, scalar.Equals(scalar.NewDate64Scalar(arrow.Date64(1024*millisInDay)), d64), "got %s", d64)
	tms, err := scalar.NewDate64Scalar(arrow.Date64(1024 * millisInDay)).CastTo(arrow.FixedWidthTypes.Timestamp_ms)
	assert.NoError(t, err)
	assert.True(t, scalar.Equals(tms, scalar.NewTimestampScalar(arrow.Timestamp(1024*millisInDay), arrow.FixedWidthTypes.Timestamp_ms)))

	tms, err = scalar.NewDate32Scalar(arrow.Date32(1024)).CastTo(arrow.FixedWidthTypes.Timestamp_ms)
	assert.NoError(t, err)
	assert.True(t, scalar.Equals(tms, scalar.NewTimestampScalar(arrow.Timestamp(1024*millisInDay), arrow.FixedWidthTypes.Timestamp_ms)))
}

func TestDurationScalarBasics(t *testing.T) {
	typ1 := arrow.FixedWidthTypes.Duration_ms
	typ2 := arrow.FixedWidthTypes.Duration_s

	val1 := arrow.Duration(1)
	val2 := arrow.Duration(2)
	tsVal1 := scalar.NewDurationScalar(val1, typ1)
	tsVal2 := scalar.NewDurationScalar(val2, typ2)
	tsNull := scalar.MakeNullScalar(typ1)
	assert.NoError(t, tsVal1.ValidateFull())
	assert.NoError(t, tsVal2.ValidateFull())
	assert.NoError(t, tsNull.ValidateFull())

	assert.Equal(t, val1, tsVal1.Value)

	assert.True(t, arrow.TypeEqual(tsVal1.Type, typ1))
	assert.True(t, arrow.TypeEqual(tsVal2.DataType(), typ2))
	assert.True(t, tsVal1.Valid)
	assert.False(t, tsNull.IsValid())
	assert.True(t, arrow.TypeEqual(typ1, tsNull.DataType()))

	assert.False(t, scalar.Equals(tsVal1, tsVal2))
	assert.False(t, scalar.Equals(tsVal1, tsNull))
	assert.False(t, scalar.Equals(tsNull, tsVal2))
}

func TestMonthIntervalScalarBasics(t *testing.T) {
	typ1 := arrow.FixedWidthTypes.MonthInterval
	typ2 := arrow.FixedWidthTypes.MonthInterval

	val1 := arrow.MonthInterval(1)
	val2 := arrow.MonthInterval(2)
	tsVal1 := scalar.NewMonthIntervalScalar(val1)
	tsVal2 := scalar.NewMonthIntervalScalar(val2)
	tsNull := scalar.MakeNullScalar(typ1)
	assert.NoError(t, tsVal1.ValidateFull())
	assert.NoError(t, tsVal2.ValidateFull())
	assert.NoError(t, tsNull.ValidateFull())

	assert.Equal(t, val1, tsVal1.Value)

	assert.True(t, arrow.TypeEqual(tsVal1.Type, typ1))
	assert.True(t, arrow.TypeEqual(tsVal2.DataType(), typ2))
	assert.True(t, tsVal1.Valid)
	assert.False(t, tsNull.IsValid())
	assert.True(t, arrow.TypeEqual(typ1, tsNull.DataType()))

	assert.False(t, scalar.Equals(tsVal1, tsVal2))
	assert.False(t, scalar.Equals(tsVal1, tsNull))
	assert.False(t, scalar.Equals(tsNull, tsVal2))
}

func TestDayTimeIntervalScalarBasics(t *testing.T) {
	typ := arrow.FixedWidthTypes.DayTimeInterval

	val1 := arrow.DayTimeInterval{Days: 1, Milliseconds: 1}
	val2 := arrow.DayTimeInterval{Days: 2, Milliseconds: 2}
	tsVal1 := scalar.NewDayTimeIntervalScalar(val1)
	tsVal2 := scalar.NewDayTimeIntervalScalar(val2)
	tsNull := scalar.MakeNullScalar(typ)
	assert.NoError(t, tsVal1.ValidateFull())
	assert.NoError(t, tsVal2.ValidateFull())
	assert.NoError(t, tsNull.ValidateFull())

	assert.Equal(t, val1, tsVal1.Value)

	assert.True(t, arrow.TypeEqual(tsVal1.Type, typ))
	assert.True(t, arrow.TypeEqual(tsVal2.DataType(), typ))
	assert.True(t, tsVal1.Valid)
	assert.False(t, tsNull.IsValid())
	assert.True(t, arrow.TypeEqual(typ, tsNull.DataType()))

	assert.False(t, scalar.Equals(tsVal1, tsVal2))
	assert.False(t, scalar.Equals(tsVal1, tsNull))
	assert.False(t, scalar.Equals(tsNull, tsVal2))
}

func TestMonthDayNanoIntervalScalarBasics(t *testing.T) {
	typ := arrow.FixedWidthTypes.MonthDayNanoInterval

	val1 := arrow.MonthDayNanoInterval{Months: 1, Days: 2, Nanoseconds: 3000}
	val2 := arrow.MonthDayNanoInterval{Months: 2, Days: 3, Nanoseconds: 4000}
	tsVal1 := scalar.NewMonthDayNanoIntervalScalar(val1)
	tsVal2 := scalar.NewMonthDayNanoIntervalScalar(val2)
	tsNull := scalar.MakeNullScalar(typ)
	assert.NoError(t, tsVal1.ValidateFull())
	assert.NoError(t, tsVal2.ValidateFull())
	assert.NoError(t, tsNull.ValidateFull())

	assert.Equal(t, val1, tsVal1.Value)

	assert.True(t, arrow.TypeEqual(tsVal1.Type, typ))
	assert.True(t, arrow.TypeEqual(tsVal2.DataType(), typ))
	assert.True(t, tsVal1.Valid)
	assert.False(t, tsNull.IsValid())
	assert.True(t, arrow.TypeEqual(typ, tsNull.DataType()))

	assert.False(t, scalar.Equals(tsVal1, tsVal2))
	assert.False(t, scalar.Equals(tsVal1, tsNull))
	assert.False(t, scalar.Equals(tsNull, tsVal2))
}

func TestNumericScalarCasts(t *testing.T) {
	tests := []arrow.DataType{
		arrow.PrimitiveTypes.Int8,
		arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Uint8,
		arrow.PrimitiveTypes.Uint16,
		arrow.PrimitiveTypes.Uint32,
		arrow.PrimitiveTypes.Uint64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
		arrow.FixedWidthTypes.Float16,
	}

	temporalTypes := []arrow.DataType{
		arrow.FixedWidthTypes.Date32,
		arrow.FixedWidthTypes.Date64,
		arrow.FixedWidthTypes.Date64,
		arrow.FixedWidthTypes.Time32ms,
		arrow.FixedWidthTypes.Time64us,
		arrow.FixedWidthTypes.Timestamp_ms,
		arrow.FixedWidthTypes.MonthInterval,
	}

	falseScalar := scalar.NewBooleanScalar(false)
	trueScalar := scalar.NewBooleanScalar(true)
	nullBool := scalar.MakeNullScalar(arrow.FixedWidthTypes.Boolean)

	for _, tt := range tests {
		t.Run(tt.ID().String()+"from bool", func(t *testing.T) {
			zero, _ := scalar.ParseScalar(tt, "0")
			zeroFromBool, err := falseScalar.CastTo(tt)
			assert.NoError(t, err)
			assert.True(t, scalar.Equals(zero, zeroFromBool))

			one, _ := scalar.ParseScalar(tt, "1")
			oneFromBool, err := trueScalar.CastTo(tt)
			assert.NoError(t, err)
			assert.True(t, scalar.Equals(one, oneFromBool))
		})
		t.Run(tt.ID().String(), func(t *testing.T) {
			for _, repr := range []string{"0", "1", "3"} {
				nullTest := scalar.MakeNullScalar(tt)
				assert.Equal(t, "null", nullTest.String())

				castedNull, err := nullBool.CastTo(tt)
				assert.NoError(t, err)
				assert.True(t, scalar.Equals(castedNull, nullTest))

				s, err := scalar.ParseScalar(tt, repr)
				assert.NoError(t, err)

				for _, other := range []arrow.DataType{arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Uint32} {
					otherNull, err := nullTest.CastTo(other)
					assert.NoError(t, err)

					expectedNull := scalar.MakeNullScalar(other)
					assert.True(t, scalar.Equals(otherNull, expectedNull))

					otherScalar, err := scalar.ParseScalar(other, repr)
					assert.NoError(t, err)

					castToOther, err := s.CastTo(other)
					assert.NoError(t, err)
					assert.True(t, scalar.Equals(castToOther, otherScalar))

					castFromOther, err := otherScalar.CastTo(tt)
					assert.NoError(t, err)
					assert.True(t, scalar.Equals(castFromOther, s))
				}

				castToBool, err := s.CastTo(arrow.FixedWidthTypes.Boolean)
				assert.NoError(t, err)
				assert.True(t, castToBool.IsValid())
				assert.Equal(t, repr != "0", castToBool.(*scalar.Boolean).Value)

				castFromStr, err := scalar.NewStringScalar(repr).CastTo(tt)
				assert.NoError(t, err)

				assert.True(t, scalar.Equals(castFromStr, s))
				assert.Equal(t, repr, s.String())
				if tt == arrow.FixedWidthTypes.Float16 {
					continue
				}

				for _, tmtyp := range temporalTypes {
					castToTemporal, err := s.CastTo(tmtyp)
					assert.NoError(t, err)
					assert.NoError(t, castToTemporal.ValidateFull())
					assert.True(t, arrow.TypeEqual(tmtyp, castToTemporal.DataType()))
				}

				if tt == arrow.PrimitiveTypes.Float32 || tt == arrow.PrimitiveTypes.Float64 {
					continue
				}

				castToStr, err := s.CastTo(arrow.BinaryTypes.String)
				assert.NoError(t, err)
				assert.Equal(t, repr, string(castToStr.(*scalar.String).Value.Bytes()))
			}
		})
	}
}

type ListScalarSuite struct {
	suite.Suite

	typ arrow.DataType
	val arrow.Array
}

func (l *ListScalarSuite) SetupTest() {
	bld := array.NewInt16Builder(memory.DefaultAllocator)
	defer bld.Release()
	bld.AppendValues([]int16{1, 2, 0}, []bool{true, true, false})

	l.val = bld.NewInt16Array()
}

func (l *ListScalarSuite) TearDownTest() {
	l.val.Release()
}

func (l *ListScalarSuite) TestBasics() {
	s, err := scalar.MakeScalarParam(l.val, l.typ)
	l.NoError(err)

	l.NoError(s.ValidateFull())
	l.True(s.IsValid())
	l.True(arrow.TypeEqual(l.typ, s.DataType()))

	nullScalar := checkMakeNullScalar(l.T(), l.typ)
	l.NoError(nullScalar.ValidateFull())
	l.False(nullScalar.IsValid())
	l.True(arrow.TypeEqual(nullScalar.DataType(), l.typ))

	l.Equal("[1 2 (null)]", s.String())
}

func (l *ListScalarSuite) TestValidateErrors() {
	// inconsistent isvalid / value
	s, _ := scalar.MakeScalarParam(l.val, l.typ)
	switch s := s.(type) {
	case *scalar.List:
		s.Valid = false
	case *scalar.FixedSizeList:
		s.Valid = false
	}
	l.Error(s.Validate())

	s, _ = scalar.MakeScalarParam(l.val, l.typ)
	switch s := s.(type) {
	case *scalar.List:
		s.Value = nil
	case *scalar.FixedSizeList:
		s.Value = nil
	}
	l.Error(s.Validate())

	// inconsistent child type
	bld := array.NewInt32Builder(memory.DefaultAllocator)
	defer bld.Release()
	bld.AppendValues([]int32{1, 2, 0}, []bool{true, true, false})
	arr := bld.NewArray()
	defer arr.Release()

	s, _ = scalar.MakeScalarParam(l.val, l.typ)
	switch s := s.(type) {
	case *scalar.List:
		s.Value = arr
	case *scalar.FixedSizeList:
		s.Value = arr
	}
	l.Error(s.Validate())
}

func TestListScalars(t *testing.T) {
	ls := new(ListScalarSuite)
	ls.typ = arrow.ListOf(arrow.PrimitiveTypes.Int16)
	suite.Run(t, ls)
	ls.typ = arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Int16)
	suite.Run(t, ls)
}

func TestFixedSizeListScalarWrongNumber(t *testing.T) {
	typ := arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Int16)
	bld := array.NewInt16Builder(memory.DefaultAllocator)
	defer bld.Release()
	bld.AppendValues([]int16{1, 2, 5}, nil)
	arr := bld.NewArray()
	defer arr.Release()

	sc := scalar.NewFixedSizeListScalarWithType(arr, typ)
	assert.NoError(t, sc.ValidateFull())

	sc.Type = arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Int16)
	assert.Error(t, sc.ValidateFull())
}

func TestMapScalarBasics(t *testing.T) {
	bld := array.NewStructBuilder(memory.DefaultAllocator, arrow.StructOf(
		arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: false},
		arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int8, Nullable: true}))
	defer bld.Release()
	bld.FieldBuilder(0).(*array.StringBuilder).AppendValues([]string{"a", "b"}, nil)
	bld.FieldBuilder(1).(*array.Int8Builder).AppendValues([]int8{1, 2}, nil)
	value := bld.NewArray()
	defer value.Release()

	s := scalar.NewMapScalar(value)
	assert.NoError(t, s.ValidateFull())

	expectedScalarType := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int8)
	assert.True(t, arrow.TypeEqual(s.DataType(), expectedScalarType))
	assert.True(t, array.Equal(value, s.GetList()))

	checkMakeNullScalar(t, expectedScalarType)
}

func TestStructScalar(t *testing.T) {
	abc := scalar.NewStructScalar([]scalar.Scalar{
		scalar.MakeScalar(true),
		scalar.MakeNullScalar(arrow.PrimitiveTypes.Int32),
		scalar.MakeScalar("hello"),
		scalar.MakeNullScalar(arrow.PrimitiveTypes.Int64),
	}, arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "c", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "d", Type: arrow.PrimitiveTypes.Int64, Nullable: true}))

	assert.NoError(t, abc.Validate())
	assert.NoError(t, abc.ValidateFull())

	a, err := abc.Field("a")
	assert.NoError(t, err)
	assert.True(t, scalar.Equals(a, abc.Value[0]))

	_, err = abc.Field("f")
	assert.Error(t, err)

	d, err := abc.Field("d")
	assert.NoError(t, err)
	assert.True(t, scalar.Equals(scalar.MakeNullScalar(arrow.PrimitiveTypes.Int64), d))
	assert.False(t, scalar.Equals(scalar.MakeScalar(int64(12)), d))

	abc2, err := scalar.NewStructScalarWithNames(abc.Value, []string{"a", "b", "c", "d"})
	assert.NoError(t, err)
	assert.True(t, scalar.Equals(abc, abc2))

	assert.Equal(t, "{a:bool = true, b:int32 = null, c:utf8 = hello, d:int64 = null}", abc.String())
}

func TestNullStructScalar(t *testing.T) {
	ty := arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "c", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "d", Type: arrow.PrimitiveTypes.Int64, Nullable: true})
	nullScalar := scalar.MakeNullScalar(ty)
	assert.NoError(t, nullScalar.ValidateFull())
	assert.False(t, nullScalar.IsValid())

	sc := checkMakeNullScalar(t, ty)
	assert.True(t, scalar.Equals(nullScalar, sc))
}

func TestStructScalarValidateErrors(t *testing.T) {
	ty := arrow.StructOf(arrow.Field{Name: "a", Type: arrow.BinaryTypes.String})

	// inconsistent isvalid value
	sc := scalar.NewStructScalar([]scalar.Scalar{scalar.MakeScalar("hello")}, ty)
	sc.Valid = false
	assert.Error(t, sc.ValidateFull())

	sc = scalar.NewStructScalar(nil, ty)
	sc.Valid = true
	assert.Error(t, sc.ValidateFull())

	// inconsistent number of fields
	sc = scalar.NewStructScalar([]scalar.Scalar{}, ty)
	assert.Error(t, sc.ValidateFull())

	sc = scalar.NewStructScalar([]scalar.Scalar{scalar.MakeScalar("foo"), scalar.MakeScalar("bar")}, ty)
	assert.Error(t, sc.ValidateFull())

	// inconsistent child value type
	sc = scalar.NewStructScalar([]scalar.Scalar{scalar.MakeScalar(42)}, ty)
	assert.Error(t, sc.ValidateFull())

	// child value has invalid utf8 data
	sc = scalar.NewStructScalar([]scalar.Scalar{scalar.MakeScalar("\xff")}, ty)
	assert.NoError(t, sc.Validate())
	assert.Error(t, sc.ValidateFull())
}

func getScalars(mem memory.Allocator) []scalar.Scalar {
	hello := memory.NewBufferBytes([]byte("hello"))
	daytime := arrow.DayTimeInterval{Days: 1, Milliseconds: 100}
	monthdaynano := arrow.MonthDayNanoInterval{Months: 5, Days: 4, Nanoseconds: 100}

	int8Bldr := array.NewInt8Builder(mem)
	defer int8Bldr.Release()

	int8Bldr.AppendValues([]int8{1, 2, 3, 4}, nil)
	int8Arr := int8Bldr.NewInt8Array()
	defer int8Arr.Release()

	mapBldr := array.NewMapBuilder(mem, arrow.PrimitiveTypes.Int8, arrow.BinaryTypes.String, false)
	defer mapBldr.Release()

	kb := mapBldr.KeyBuilder().(*array.Int8Builder)
	ib := mapBldr.ItemBuilder().(*array.StringBuilder)

	mapBldr.Append(true)
	kb.AppendValues([]int8{1, 2, 3}, nil)
	ib.AppendValues([]string{"foo", "bar", "baz"}, nil)

	mapArr := mapBldr.NewMapArray()
	defer mapArr.Release()

	return []scalar.Scalar{
		scalar.NewBooleanScalar(false),
		scalar.NewInt8Scalar(3),
		scalar.NewUint16Scalar(3),
		scalar.NewInt32Scalar(3),
		scalar.NewUint64Scalar(3),
		scalar.NewFloat64Scalar(3.0),
		scalar.NewDate32Scalar(10),
		scalar.NewDate64Scalar(11),
		scalar.NewTime32Scalar(1000, arrow.FixedWidthTypes.Time32s),
		scalar.NewTime64Scalar(1111, arrow.FixedWidthTypes.Time64us),
		scalar.NewTimestampScalar(111, arrow.FixedWidthTypes.Timestamp_ms),
		scalar.NewMonthIntervalScalar(1),
		scalar.NewDayTimeIntervalScalar(daytime),
		scalar.NewMonthDayNanoIntervalScalar(monthdaynano),
		scalar.NewDurationScalar(60, arrow.FixedWidthTypes.Duration_s),
		scalar.NewBinaryScalar(hello, arrow.BinaryTypes.Binary),
		scalar.NewFixedSizeBinaryScalar(hello, &arrow.FixedSizeBinaryType{ByteWidth: hello.Len()}),
		scalar.NewDecimal128Scalar(decimal128.FromI64(10), &arrow.Decimal128Type{Precision: 16, Scale: 4}),
		scalar.NewStringScalarFromBuffer(hello),
		scalar.NewListScalar(int8Arr),
		scalar.NewMapScalar(mapArr.List.ListValues()),
		scalar.NewFixedSizeListScalar(int8Arr),
		scalar.NewStructScalar([]scalar.Scalar{scalar.NewInt32Scalar(2), scalar.NewInt32Scalar(6)},
			arrow.StructOf([]arrow.Field{{Name: "min", Type: arrow.PrimitiveTypes.Int32}, {Name: "max", Type: arrow.PrimitiveTypes.Int32}}...)),
		scalar.NewRunEndEncodedScalar(scalar.NewStringScalarFromBuffer(hello),
			arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int32, arrow.BinaryTypes.String)),
	}
}

func TestMakeArrayFromScalar(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	nullArray, err := scalar.MakeArrayFromScalar(scalar.ScalarNull, 5, mem)
	assert.NoError(t, err)
	defer nullArray.Release()

	assert.Equal(t, 5, nullArray.Len())
	assert.Equal(t, 5, nullArray.NullN())

	scalars := getScalars(mem)

	for _, length := range []int{16} {
		for _, s := range scalars {
			t.Run(s.DataType().Name(), func(t *testing.T) {
				if ls, ok := s.(scalar.Releasable); ok {
					defer ls.Release()
				}

				arr, err := scalar.MakeArrayFromScalar(s, length, mem)
				assert.NoError(t, err)
				defer arr.Release()

				assert.Equal(t, length, arr.Len())
				assert.Zero(t, arr.NullN())

				for _, i := range []int{0, length / 2, length - 1} {
					scalarCompare, err := scalar.GetScalar(arr, i)
					assert.NoError(t, err)
					assert.True(t, scalar.Equals(s, scalarCompare))
					if ls, ok := scalarCompare.(scalar.Releasable); ok {
						ls.Release()
					}
				}
			})
		}
	}
}

type OptionListTest struct {
	FieldNames []string          `compute:"field_names"`
	FieldNulls []bool            `compute:"field_null"`
	FieldMeta  []*arrow.Metadata `compute:"field_metadata"`
	Val8       []int8            `compute:"val8"`
	ValU8      []uint8           `compute:"u8"`
	Val16      []int16           `compute:"val16"`
	ValU16     []uint16          `compute:"u16"`
	Val32      []int32           `compute:"val32"`
	ValU32     []uint32          `compute:"u32"`
	Val64      []int64           `compute:"val64"`
	ValU64     []uint64          `compute:"u64"`
	ValInt     []int             `compute:"valint"`
	ValUint    []uint            `compute:"valuint"`
}

type OptionValTest struct {
	ToType arrow.DataType `compute:"type"`
	Allow  bool           `compute:"allow"`
}

func (OptionValTest) TypeName() string { return "OptionValTest" }

func TestToScalar(t *testing.T) {
	ot := &OptionValTest{ToType: arrow.BinaryTypes.String, Allow: true}
	sc, err := scalar.ToScalar(ot, memory.DefaultAllocator)
	assert.NoError(t, err)
	assert.Equal(t, `{type:utf8 = null, allow:bool = true, _type_name:binary = OptionValTest}`, sc.String())

	meta := arrow.MetadataFrom(map[string]string{
		"option":  "val",
		"captain": "planet",
		"souper":  "bowl",
	})

	olt := OptionListTest{
		FieldNames: []string{"foo", "bar", "baz"},
		FieldNulls: []bool{true, false},
		FieldMeta:  []*arrow.Metadata{&meta, nil, &meta},
		Val8:       []int8{1, 2, 3, 4},
		ValU8:      []uint8{5, 6},
		Val16:      []int16{7, 8, 9, 10},
		ValU16:     []uint16{},
		Val32:      nil,
		ValU32:     []uint32{25, 26, 27, 28},
		Val64:      []int64{-1, -2, -3, -4, -5},
		ValU64:     []uint64{1, 2, 3},
		ValInt:     []int{10, 11, 12, 13},
		ValUint:    []uint{14, 15, 16},
	}
	sc, err = scalar.ToScalar(olt, memory.DefaultAllocator)
	assert.NoError(t, err)

	expected := `{field_names:list<item: utf8, nullable> = ["foo" "bar" "baz"], ` +
		`field_null:list<item: bool, nullable> = [true false], ` +
		`field_metadata:list<item: map<binary, binary, items_nullable>, nullable> = ` +
		`[{["captain" "option" "souper"] ["planet" "val" "bowl"]} {[] []} {["captain" "option" "souper"] ["planet" "val" "bowl"]}], ` +
		`val8:list<item: int8, nullable> = [1 2 3 4], ` +
		`u8:list<item: uint8, nullable> = [5 6], ` +
		`val16:list<item: int16, nullable> = [7 8 9 10], ` +
		`u16:list<item: uint16, nullable> = [], ` +
		`val32:list<item: int32, nullable> = [], ` +
		`u32:list<item: uint32, nullable> = [25 26 27 28], ` +
		`val64:list<item: int64, nullable> = [-1 -2 -3 -4 -5], ` +
		`u64:list<item: uint64, nullable> = [1 2 3], ` +
		`valint:list<item: int64, nullable> = [10 11 12 13], ` +
		`valuint:list<item: uint64, nullable> = [14 15 16]}`

	assert.Equal(t, expected, sc.String())
}

var dictIndexTypes = []arrow.DataType{
	arrow.PrimitiveTypes.Int8,
	arrow.PrimitiveTypes.Uint8,
	arrow.PrimitiveTypes.Int16,
	arrow.PrimitiveTypes.Uint16,
	arrow.PrimitiveTypes.Int32,
	arrow.PrimitiveTypes.Uint32,
	arrow.PrimitiveTypes.Int64,
	arrow.PrimitiveTypes.Uint64,
}

func TestDictionaryScalarBasics(t *testing.T) {
	for _, indexType := range dictIndexTypes {
		t.Run(fmt.Sprint(indexType), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			ty := &arrow.DictionaryType{IndexType: indexType, ValueType: arrow.BinaryTypes.String}
			dict, _, _ := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["alpha", null, "gamma"]`))
			defer dict.Release()

			idxScalar, _ := scalar.MakeScalarParam(0, indexType)
			alpha := scalar.NewDictScalar(idxScalar, dict)
			defer alpha.Release()

			idxScalar, _ = scalar.MakeScalarParam(2, indexType)
			gamma := scalar.NewDictScalar(idxScalar, dict)
			defer gamma.Release()

			idxScalar, _ = scalar.MakeScalarParam(1, indexType)
			nullVal := scalar.NewDictScalar(idxScalar, dict)
			defer nullVal.Release()

			scalarNull := scalar.MakeNullScalar(ty)
			scalarNull.(*scalar.Dictionary).Value.Dict = dict
			dict.Retain()
			defer scalarNull.(*scalar.Dictionary).Release()

			assert.NoError(t, scalarNull.ValidateFull())
			assert.NoError(t, alpha.ValidateFull())
			assert.NoError(t, gamma.ValidateFull())

			// index is valid, corresponding value is null
			assert.NoError(t, nullVal.ValidateFull())

			encodedNull, err := scalarNull.(*scalar.Dictionary).GetEncodedValue()
			assert.NoError(t, err)
			assert.NoError(t, encodedNull.ValidateFull())
			assert.True(t, scalar.Equals(encodedNull, scalar.MakeNullScalar(arrow.BinaryTypes.String)))

			encodedNullVal, err := nullVal.GetEncodedValue()
			assert.NoError(t, err)
			assert.NoError(t, encodedNullVal.ValidateFull())
			assert.True(t, scalar.Equals(encodedNullVal, scalar.MakeNullScalar(arrow.BinaryTypes.String)))

			encodedAlpha, err := alpha.GetEncodedValue()
			assert.NoError(t, err)
			assert.NoError(t, encodedAlpha.ValidateFull())
			assert.True(t, scalar.Equals(encodedAlpha, scalar.MakeScalar("alpha")))

			encodedGamma, err := gamma.GetEncodedValue()
			assert.NoError(t, err)
			assert.NoError(t, encodedGamma.ValidateFull())
			assert.True(t, scalar.Equals(encodedGamma, scalar.MakeScalar("gamma")))

			idxArr, _, _ := array.FromJSON(mem, indexType, strings.NewReader(`[2, 0, 1, null]`))
			defer idxArr.Release()
			arr := array.NewDictionaryArray(ty, idxArr, dict)
			defer arr.Release()

			first, err := scalar.GetScalar(arr, 0)
			assert.NoError(t, err)
			second, err := scalar.GetScalar(arr, 1)
			assert.NoError(t, err)
			third, err := scalar.GetScalar(arr, 2)
			assert.NoError(t, err)
			last, err := scalar.GetScalar(arr, 3)
			assert.NoError(t, err)

			defer func() {
				first.(*scalar.Dictionary).Release()
				second.(*scalar.Dictionary).Release()
				third.(*scalar.Dictionary).Release()
				last.(*scalar.Dictionary).Release()
			}()

			assert.NoError(t, first.ValidateFull())
			assert.NoError(t, second.ValidateFull())
			assert.NoError(t, third.ValidateFull())
			assert.NoError(t, last.ValidateFull())

			assert.True(t, first.IsValid())
			assert.True(t, second.IsValid())
			assert.True(t, third.IsValid()) // valid because of valid index despite null value
			assert.False(t, last.IsValid())

			assert.True(t, scalar.Equals(first, gamma))
			assert.True(t, scalar.Equals(second, alpha))
			assert.True(t, scalar.Equals(third, nullVal))
			assert.True(t, scalar.Equals(last, scalarNull))

			assert.Same(t, first.(*scalar.Dictionary).Value.Dict, arr.Dictionary())
			assert.Same(t, second.(*scalar.Dictionary).Value.Dict, arr.Dictionary())
		})
	}
}

func TestDictionaryScalarValidateErrors(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	var (
		indexTy = arrow.PrimitiveTypes.Int16
		valueTy = arrow.BinaryTypes.String
		dictTy  = &arrow.DictionaryType{IndexType: indexTy, ValueType: valueTy}
	)

	dict, _, _ := array.FromJSON(mem, valueTy, strings.NewReader(`["alpha", null, "gamma"]`))
	defer dict.Release()

	alpha := scalar.NewDictScalar(scalar.MakeScalar(int16(0)), dict)
	defer alpha.Release()

	// Valid index, null underlying value
	nullVal := scalar.NewDictScalar(scalar.MakeScalar(int16(1)), dict)
	defer nullVal.Release()

	// inconsistent index type
	dictSc := scalar.NewDictScalar(alpha.Value.Index, dict)
	defer dictSc.Release()
	dictSc.Type = &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: valueTy}
	assert.Error(t, dictSc.Validate())

	// inconsistent value type between dict and type
	dictSc.Type = &arrow.DictionaryType{IndexType: indexTy, ValueType: arrow.BinaryTypes.Binary}
	assert.Error(t, dictSc.Validate())

	// inconsistent Valid/Value
	dictSc.Type = dictTy
	assert.NoError(t, dictSc.ValidateFull())
	dictSc.Valid = false
	assert.Error(t, dictSc.ValidateFull())

	assert.NoError(t, nullVal.ValidateFull())
	nullVal.Valid = false
	assert.Error(t, nullVal.ValidateFull())

	dictSc = scalar.NewNullDictScalar(dictTy)
	dictSc.Valid = true
	assert.Error(t, dictSc.ValidateFull())
	dictSc.Valid = false
	assert.NoError(t, dictSc.ValidateFull())

	// index value out of bounds
	for _, idx := range []int16{-1, 3} {
		invalid := scalar.NewDictScalar(scalar.MakeScalar(idx), dict)
		defer invalid.Release()

		assert.NoError(t, invalid.Validate())
		assert.Error(t, invalid.ValidateFull())
	}
}

func checkGetValidUnionScalar(t *testing.T, arr arrow.Array, idx int, expected, expectedValue scalar.Scalar) {
	s, err := scalar.GetScalar(arr, idx)
	assert.NoError(t, err)
	assert.NoError(t, s.ValidateFull())
	assert.True(t, scalar.Equals(expected, s))

	assert.True(t, s.IsValid())
	assert.True(t, scalar.Equals(s.(scalar.Union).ChildValue(), expectedValue), s, expectedValue)
}

func checkGetNullUnionScalar(t *testing.T, arr arrow.Array, idx int) {
	s, err := scalar.GetScalar(arr, idx)
	assert.NoError(t, err)
	assert.True(t, scalar.Equals(scalar.MakeNullScalar(arr.DataType()), s))
	assert.False(t, s.IsValid())
	assert.False(t, s.(scalar.Union).ChildValue().IsValid())
}

func makeSparseUnionScalar(ty *arrow.SparseUnionType, val scalar.Scalar, idx int) scalar.Scalar {
	return scalar.NewSparseUnionScalarFromValue(val, idx, ty)
}

func makeDenseUnionScalar(ty *arrow.DenseUnionType, val scalar.Scalar, idx int) scalar.Scalar {
	return scalar.NewDenseUnionScalar(val, ty.TypeCodes()[idx], ty)
}

func makeSpecificNullScalar(dt arrow.UnionType, idx int) scalar.Scalar {
	switch dt.Mode() {
	case arrow.SparseMode:
		values := make([]scalar.Scalar, len(dt.Fields()))
		for i, f := range dt.Fields() {
			values[i] = scalar.MakeNullScalar(f.Type)
		}
		return scalar.NewSparseUnionScalar(values, dt.TypeCodes()[idx], dt.(*arrow.SparseUnionType))
	case arrow.DenseMode:
		code := dt.TypeCodes()[idx]
		value := scalar.MakeNullScalar(dt.Fields()[idx].Type)
		return scalar.NewDenseUnionScalar(value, code, dt.(*arrow.DenseUnionType))
	}
	return nil
}

type UnionScalarSuite struct {
	suite.Suite

	mode                                            arrow.UnionMode
	dt                                              arrow.DataType
	unionType                                       arrow.UnionType
	alpha, beta, two, three                         scalar.Scalar
	unionAlpha, unionBeta, unionTwo, unionThree     scalar.Scalar
	unionOtherTwo, unionStringNull, unionNumberNull scalar.Scalar
}

func (s *UnionScalarSuite) scalarFromValue(idx int, val scalar.Scalar) scalar.Scalar {
	switch s.mode {
	case arrow.SparseMode:
		return makeSparseUnionScalar(s.dt.(*arrow.SparseUnionType), val, idx)
	case arrow.DenseMode:
		return makeDenseUnionScalar(s.dt.(*arrow.DenseUnionType), val, idx)
	}
	return nil
}

func (s *UnionScalarSuite) specificNull(idx int) scalar.Scalar {
	return makeSpecificNullScalar(s.unionType, idx)
}

func (s *UnionScalarSuite) SetupTest() {
	s.dt = arrow.UnionOf(s.mode, []arrow.Field{
		{Name: "string", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "number", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
		{Name: "other_number", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
	}, []arrow.UnionTypeCode{3, 42, 43})

	s.unionType = s.dt.(arrow.UnionType)

	s.alpha = scalar.MakeScalar("alpha")
	s.beta = scalar.MakeScalar("beta")
	s.two = scalar.MakeScalar(uint64(2))
	s.three = scalar.MakeScalar(uint64(3))

	s.unionAlpha = s.scalarFromValue(0, s.alpha)
	s.unionBeta = s.scalarFromValue(0, s.beta)
	s.unionTwo = s.scalarFromValue(1, s.two)
	s.unionOtherTwo = s.scalarFromValue(2, s.two)
	s.unionThree = s.scalarFromValue(1, s.three)
	s.unionStringNull = s.specificNull(0)
	s.unionNumberNull = s.specificNull(1)
}

func (s *UnionScalarSuite) TestValidate() {
	s.NoError(s.unionAlpha.ValidateFull())
	s.NoError(s.unionAlpha.Validate())
	s.NoError(s.unionBeta.ValidateFull())
	s.NoError(s.unionBeta.Validate())
	s.NoError(s.unionTwo.ValidateFull())
	s.NoError(s.unionTwo.Validate())
	s.NoError(s.unionOtherTwo.ValidateFull())
	s.NoError(s.unionOtherTwo.Validate())
	s.NoError(s.unionThree.ValidateFull())
	s.NoError(s.unionThree.Validate())
	s.NoError(s.unionStringNull.ValidateFull())
	s.NoError(s.unionStringNull.Validate())
	s.NoError(s.unionNumberNull.ValidateFull())
	s.NoError(s.unionNumberNull.Validate())
}

func (s *UnionScalarSuite) setTypeCode(sc scalar.Scalar, c arrow.UnionTypeCode) {
	switch sc := sc.(type) {
	case *scalar.SparseUnion:
		sc.TypeCode = c
	case *scalar.DenseUnion:
		sc.TypeCode = c
	}
}

func (s *UnionScalarSuite) setIsValid(sc scalar.Scalar, v bool) {
	switch sc := sc.(type) {
	case *scalar.SparseUnion:
		sc.Valid = v
	case *scalar.DenseUnion:
		sc.Valid = v
	}
}

func (s *UnionScalarSuite) TestValidateErrors() {
	// type code doesn't exist
	sc := s.scalarFromValue(0, s.alpha)

	// invalid type code
	s.setTypeCode(sc, 0)
	s.Error(sc.Validate())
	s.Error(sc.ValidateFull())

	s.setIsValid(sc, false)
	s.Error(sc.Validate())
	s.Error(sc.ValidateFull())

	s.setTypeCode(sc, -42)
	s.setIsValid(sc, true)
	s.Error(sc.Validate())
	s.Error(sc.ValidateFull())

	s.setIsValid(sc, false)
	s.Error(sc.Validate())
	s.Error(sc.ValidateFull())

	// type code doesn't correspond to child type
	if sc, ok := sc.(*scalar.DenseUnion); ok {
		sc.TypeCode = 42
		sc.Valid = true
		s.Error(sc.Validate())
		s.Error(sc.ValidateFull())

		sc = s.scalarFromValue(2, s.two).(*scalar.DenseUnion)
		sc.TypeCode = 3
		s.Error(sc.Validate())
		s.Error(sc.ValidateFull())
	}

	// underlying value has invalid utf8
	sc = s.scalarFromValue(0, scalar.NewStringScalar("\xff"))
	s.NoError(sc.Validate())
	s.Error(sc.ValidateFull())
}

func (s *UnionScalarSuite) TestEquals() {
	// differing values
	s.False(scalar.Equals(s.unionAlpha, s.unionBeta))
	s.False(scalar.Equals(s.unionTwo, s.unionThree))
	// differing validities
	s.False(scalar.Equals(s.unionAlpha, s.unionStringNull))
	// differing types
	s.False(scalar.Equals(s.unionAlpha, s.unionTwo))
	s.False(scalar.Equals(s.unionAlpha, s.unionOtherTwo))
	// type codes don't count when comparing union scalars: the underlying
	// values are identical even though their provenance is different
	s.True(scalar.Equals(s.unionTwo, s.unionOtherTwo))
	s.True(scalar.Equals(s.unionStringNull, s.unionNumberNull))
}

func (s *UnionScalarSuite) TestMakeNullScalar() {
	sc := scalar.MakeNullScalar(s.dt)
	s.True(arrow.TypeEqual(s.dt, sc.DataType()))
	s.False(sc.IsValid())

	// the first child field is chosen arbitrarily for the purposes of
	// making a null scalar
	switch s.mode {
	case arrow.DenseMode:
		asDense := sc.(*scalar.DenseUnion)
		s.EqualValues(3, asDense.TypeCode)
		s.False(asDense.Value.IsValid())
	case arrow.SparseMode:
		asSparse := sc.(*scalar.SparseUnion)
		s.EqualValues(3, asSparse.TypeCode)
		s.False(asSparse.Value[asSparse.ChildID].IsValid())
	}
}

type SparseUnionSuite struct {
	UnionScalarSuite
}

func (s *SparseUnionSuite) SetupSuite() {
	s.mode = arrow.SparseMode
}

func (s *SparseUnionSuite) TestGetScalar() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(s.T(), 0)

	children := make([]arrow.Array, 3)
	children[0], _, _ = array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["alpha", "", "beta", null, "gamma"]`))
	defer children[0].Release()
	children[1], _, _ = array.FromJSON(mem, arrow.PrimitiveTypes.Uint64, strings.NewReader(`[1, 2, 11, 22, null]`))
	defer children[1].Release()
	children[2], _, _ = array.FromJSON(mem, arrow.PrimitiveTypes.Uint64, strings.NewReader(`[100, 101, 102, 103, 104]`))
	defer children[2].Release()

	typeIDs, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int8, strings.NewReader(`[3, 42, 3, 3, 42]`))
	defer typeIDs.Release()

	arr := array.NewSparseUnion(s.dt.(*arrow.SparseUnionType), 5, children, typeIDs.Data().Buffers()[1], 0)
	defer arr.Release()

	checkGetValidUnionScalar(s.T(), arr, 0, s.unionAlpha, s.alpha)
	checkGetValidUnionScalar(s.T(), arr, 1, s.unionTwo, s.two)
	checkGetValidUnionScalar(s.T(), arr, 2, s.unionBeta, s.beta)
	checkGetNullUnionScalar(s.T(), arr, 3)
	checkGetNullUnionScalar(s.T(), arr, 4)
}

type DenseUnionSuite struct {
	UnionScalarSuite
}

func (s *DenseUnionSuite) SetupSuite() {
	s.mode = arrow.DenseMode
}

func (s *DenseUnionSuite) TestGetScalar() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(s.T(), 0)

	children := make([]arrow.Array, 3)
	children[0], _, _ = array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["alpha", "beta", null]`))
	defer children[0].Release()
	children[1], _, _ = array.FromJSON(mem, arrow.PrimitiveTypes.Uint64, strings.NewReader(`[2, 3]`))
	defer children[1].Release()
	children[2], _, _ = array.FromJSON(mem, arrow.PrimitiveTypes.Uint64, strings.NewReader(`[]`))
	defer children[2].Release()

	typeIDs, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int8, strings.NewReader(`[3, 42, 3, 3, 42]`))
	defer typeIDs.Release()
	offsets, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[0, 0, 1, 2, 1]`))
	defer offsets.Release()

	arr := array.NewDenseUnion(s.dt.(*arrow.DenseUnionType), 5, children, typeIDs.Data().Buffers()[1], offsets.Data().Buffers()[1], 0)
	defer arr.Release()

	checkGetValidUnionScalar(s.T(), arr, 0, s.unionAlpha, s.alpha)
	checkGetValidUnionScalar(s.T(), arr, 1, s.unionTwo, s.two)
	checkGetValidUnionScalar(s.T(), arr, 2, s.unionBeta, s.beta)
	checkGetNullUnionScalar(s.T(), arr, 3)
	checkGetValidUnionScalar(s.T(), arr, 4, s.unionThree, s.three)
}

func TestUnionScalars(t *testing.T) {
	suite.Run(t, new(SparseUnionSuite))
	suite.Run(t, new(DenseUnionSuite))
}

func TestRunEndEncodedGetScalar(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	runEnds, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[100, 200, 300, 400, 500]`))
	defer runEnds.Release()

	values, _, _ := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["Hello", "beautiful", "world", "of", "RLE"]`))
	defer values.Release()

	reeArray := array.NewRunEndEncodedArray(runEnds, values, 500, 0)
	defer reeArray.Release()

	slice := array.NewSlice(reeArray, 199, 404).(*array.RunEndEncoded)
	defer slice.Release()

	tests := []struct {
		name  string
		arr   arrow.Array
		idx   int
		exval string
	}{
		{"simple", reeArray, 225, "world"},
		{"offset", slice, 125, "of"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc, err := scalar.GetScalar(tt.arr, tt.idx)
			require.NoError(t, err)
			reeScalar := sc.(*scalar.RunEndEncoded)
			defer reeScalar.Release()

			assert.NoError(t, reeScalar.Validate())
			expectedType := tt.arr.DataType().(*arrow.RunEndEncodedType).Encoded()
			assert.Truef(t, arrow.TypeEqual(expectedType, reeScalar.Value.DataType()),
				"expected: %s\ngot: %s", expectedType, reeScalar.Value.DataType())
			assert.Equal(t, tt.exval, reeScalar.Value.String())
		})
	}
}

func TestRunEndEncodedNullScalar(t *testing.T) {
	dt := arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int16, arrow.BinaryTypes.String)
	sc := scalar.MakeNullScalar(dt)

	assert.False(t, sc.IsValid())
	assert.Truef(t, arrow.TypeEqual(dt, sc.DataType()), "expected: %s\ngot: %s", dt, sc.DataType())
	assert.IsType(t, (*scalar.RunEndEncoded)(nil), sc)
}
