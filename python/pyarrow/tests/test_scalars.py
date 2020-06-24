# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import datetime
import pytest

import numpy as np

import pyarrow as pa


@pytest.mark.parametrize(['value', 'ty', 'klass'], [
    (None, None, pa.lib.NullScalar),
    (False, None, pa.lib.BooleanScalar),
    (True, None, pa.lib.BooleanScalar),
    (1, None, pa.lib.Int64Scalar),
    (-1, None, pa.lib.Int64Scalar),
    (1, pa.int8(), pa.lib.Int8Scalar),
    (1, pa.uint8(), pa.lib.UInt8Scalar),
    (1, pa.int16(), pa.lib.Int16Scalar),
    (1, pa.uint16(), pa.lib.UInt16Scalar),
    (1, pa.int32(), pa.lib.Int32Scalar),
    (1, pa.uint32(), pa.lib.UInt32Scalar),
    (1, pa.int64(), pa.lib.Int64Scalar),
    (1, pa.uint64(), pa.lib.UInt64Scalar),
    (1.0, None, pa.lib.DoubleScalar),
    (np.float16(1.0), pa.float16(), pa.lib.HalfFloatScalar),
    (1.0, pa.float32(), pa.lib.FloatScalar),
    ("string", None, pa.lib.StringScalar),
    (b"bytes", None, pa.lib.BinaryScalar),
    ([1, 2, 3], None, pa.lib.ListScalar),
    ([1, 2, 3, 4], pa.large_list(pa.int8()), pa.lib.LargeListScalar),
    # date
    # time
])
def test_type_inference(value, ty, klass):
    s = pa.scalar(value, type=ty)
    assert isinstance(s, klass)
    assert s == value


def test_null_singleton():
    with pytest.raises(Exception):
        pa.NullScalar()


def test_nulls():
    arr = pa.array([None, None])
    for v in arr:
        assert v is pa.NA
        assert v.as_py() is None


def test_null_equality():
    assert (pa.NA == pa.NA) is pa.NA
    assert (pa.NA == 1) is pa.NA


def test_hashing():
    # ARROW-640

    # int hashing
    int_arr = pa.array([1, 1, 2, 1])
    assert hash(int_arr[0]) == hash(1)

    # float hashing
    float_arr = pa.array([1.4, 1.2, 2.5, 1.8])
    assert hash(float_arr[0]) == hash(1.4)

    # string hashing
    str_arr = pa.array(["foo", "bar"])
    assert hash(str_arr[1]) == hash("bar")

    # binary hashing
    byte_arr = pa.array([b'foo', None, b'bar'])
    assert hash(byte_arr[2]) == hash(b"bar")

    # array to set
    arr = pa.array([1, 1, 2, 1])
    set_from_array = set(arr)
    assert isinstance(set_from_array, set)
    assert set_from_array == {1, 2}


# TODO(kszucs): test array.__getitem__ in test_array.py

def test_bool():
    false = pa.scalar(False)
    true = pa.scalar(True)

    assert isinstance(false, pa.BooleanScalar)
    assert isinstance(true, pa.BooleanScalar)

    assert repr(true) == "<pyarrow.BooleanScalar: True>"
    assert str(true) == "True"
    assert repr(false) == "<pyarrow.BooleanScalar: False>"
    assert str(false) == "False"

    assert true.as_py() is True
    assert false.as_py() is False


def test_numerics():
    # int64
    s = pa.scalar(1)
    assert isinstance(s, pa.Int64Value)
    assert repr(s) == "<pyarrow.Int64Scalar: 1>"
    assert str(s) == "1"
    assert s.as_py() == 1
    assert s == 1

    # float64
    s = pa.scalar(1.5)
    assert isinstance(s, pa.DoubleValue)
    assert repr(s) == "<pyarrow.DoubleScalar: 1.5>"
    assert str(s) == "1.5"
    assert s.as_py() == 1.5
    assert s == 1.5

    # float16
    s = pa.scalar(np.float16(0.5), type=pa.float16())
    assert isinstance(s, pa.HalfFloatValue)
    assert repr(s) == "<pyarrow.HalfFloatScalar: 0.5>"
    assert str(s) == "0.5"
    assert s.as_py() == 0.5
    assert s == 0.5


def test_date():
    # ARROW-5125
    d1 = datetime.date(3200, 1, 1)
    d2 = datetime.date(1960, 1, 1)

    for ty in [pa.date32(), pa.date64()]:
        for d in [d1, d2]:
            s = pa.scalar(d, type=ty)
            assert s == d


# TODO(kszucs): add time32/64 tests including units
# TODO(kszucs): add decimal tests including scale


@pytest.mark.pandas
def test_timestamp(self):
    import pandas as pd
    arr = pd.date_range('2000-01-01 12:34:56', periods=10).values

    units = ['ns', 'us', 'ms', 's']

    for i, unit in enumerate(units):
        dtype = 'datetime64[{}]'.format(unit)
        arrow_arr = pa.Array.from_pandas(arr.astype(dtype))
        expected = pd.Timestamp('2000-01-01 12:34:56')

        assert arrow_arr[0].as_py() == expected
        assert arrow_arr[0].value * 1000**i == expected.value

        tz = 'America/New_York'
        arrow_type = pa.timestamp(unit, tz=tz)

        dtype = 'datetime64[{}]'.format(unit)
        arrow_arr = pa.Array.from_pandas(arr.astype(dtype), type=arrow_type)
        expected = (pd.Timestamp('2000-01-01 12:34:56')
                    .tz_localize('utc')
                    .tz_convert(tz))

        assert arrow_arr[0].as_py() == expected
        assert arrow_arr[0].value * 1000**i == expected.value


@pytest.mark.nopandas
def test_timestamp_nanos_nopandas():
    # ARROW-5450
    import pytz
    tz = 'America/New_York'
    ty = pa.timestamp('ns', tz=tz)

    # 2000-01-01 00:00:00 + 1 microsecond
    s = pa.scalar(946684800000000000 + 1000, type=ty)

    tzinfo = pytz.timezone(tz)
    expected = datetime.datetime(2000, 1, 1, microsecond=1, tzinfo=tzinfo)
    expected = tzinfo.fromutc(expected)
    result = s.as_py()
    assert result == expected
    assert result.year == 1999
    assert result.hour == 19

    # Non-zero nanos yields ValueError
    s = pa.scalar(946684800000000001, type=ty)
    with pytest.raises(ValueError):
        s.as_py()


def test_timestamp_no_overflow():
    # ARROW-5450
    import pytz

    timestamps = [
        datetime.datetime(1, 1, 1, 0, 0, 0, tzinfo=pytz.utc),
        datetime.datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
        datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz.utc),
    ]
    for ts in timestamps:
        s = pa.scalar(ts, type=pa.timestamp("us", tz="UTC"))
        assert s == ts
        assert s.as_py() == ts


def test_duration():
    arr = np.array([0, 3600000000000], dtype='timedelta64[ns]')

    units = ['us', 'ms', 's']

    for i, unit in enumerate(units):
        dtype = 'timedelta64[{}]'.format(unit)
        arrow_arr = pa.array(arr.astype(dtype))
        expected = datetime.timedelta(seconds=60*60)
        assert isinstance(arrow_arr[1].as_py(), datetime.timedelta)
        assert arrow_arr[1].as_py() == expected
        assert (arrow_arr[1].value * 1000**(i+1) ==
                expected.total_seconds() * 1e9)


@pytest.mark.pandas
def test_duration_nanos_pandas():
    import pandas as pd
    arr = pa.array([0, 3600000000000], type=pa.duration('ns'))
    expected = pd.Timedelta('1 hour')
    assert isinstance(arr[1].as_py(), pd.Timedelta)
    assert arr[1].as_py() == expected
    assert arr[1].value == expected.value

    # Non-zero nanos work fine
    arr = pa.array([946684800000000001], type=pa.duration('ns'))
    assert arr[0].as_py() == pd.Timedelta(946684800000000001, unit='ns')


@pytest.mark.nopandas
def test_duration_nanos_nopandas():
    arr = pa.array([0, 3600000000000], pa.duration('ns'))
    expected = datetime.timedelta(seconds=60*60)
    assert isinstance(arr[1].as_py(), datetime.timedelta)
    assert arr[1].as_py() == expected
    assert arr[1].value == expected.total_seconds() * 1e9

    # Non-zero nanos yields ValueError
    arr = pa.array([946684800000000001], type=pa.duration('ns'))
    with pytest.raises(ValueError):
        arr[0].as_py()


@pytest.mark.parametrize('value', ['foo', 'mañana'])
@pytest.mark.parametrize(('ty', 'scalar_typ'), [
    (pa.string(), pa.StringScalar),
    (pa.large_string(), pa.LargeStringScalar)
])
def test_string(value, ty, scalar_typ):
    s = pa.scalar(value, type=ty)
    assert isinstance(s, scalar_typ)
    assert s.as_py() == value
    assert s.as_py() != 'something'
    assert repr(value) in repr(s)
    assert str(s) == str(value)

    buf = s.as_buffer()
    assert isinstance(buf, pa.Buffer)
    assert buf.to_pybytes() == value.encode()


@pytest.mark.parametrize('value', [b'foo', b'bar'])
@pytest.mark.parametrize(('ty', 'scalar_typ'), [
    (pa.binary(), pa.BinaryScalar),
    (pa.large_binary(), pa.LargeBinaryScalar)
])
def test_binary(value, ty, scalar_typ):
    s = pa.scalar(value, type=ty)
    assert isinstance(s, scalar_typ)
    assert s.as_py() == value
    assert str(s) == str(value)
    assert repr(value) in repr(s)
    assert s == value
    assert s != b'xxxxx'

    buf = s.as_buffer()
    assert isinstance(buf, pa.Buffer)
    assert buf.to_pybytes() == value


def test_fixed_size_binary():
    s = pa.scalar(b'foof', type=pa.binary(4))
    assert isinstance(s, pa.FixedSizeBinaryScalar)
    assert s.as_py() == b'foof'

    with pytest.raises(pa.ArrowInvalid):
        pa.scalar(b'foof5', type=pa.binary(4))


@pytest.mark.parametrize(('ty', 'klass'), [
    (pa.list_(pa.string()), pa.ListScalar),
    (pa.large_list(pa.string()), pa.LargeListScalar)
])
def test_list(ty, klass):
    v = ['foo', None]
    s = pa.scalar(v, type=ty)
    assert len(s) == 2
    assert isinstance(s, klass)
    assert repr(v) in repr(s)
    assert s.as_py() == v
    assert s[0].as_py() == 'foo'
    # TODO(kszucs) assert v[1] is pa.NA
    assert s[-1] == s[1]
    assert s[-2] == s[0]
    with pytest.raises(IndexError):
        s[-3]
    with pytest.raises(IndexError):
        s[2]


def test_fixed_size_list():
    s = pa.scalar([1, None, 3], type=pa.list_(pa.int64(), 3))

    assert len(s) == 3
    assert isinstance(s, pa.FixedSizeListScalar)
    assert repr(s) == "<pyarrow.FixedSizeListScalar: [1, None, 3]>"
    assert s.as_py() == [1, None, 3]
    assert s[0].as_py() == 1
    # TODO(kszucs): assert v[1] is pa.NA
    assert s[-1] == s[2]
    with pytest.raises(IndexError):
        s[-4]
    with pytest.raises(IndexError):
        s[3]


def test_struct():
    ty = pa.struct([
        pa.field('x', pa.int16()),
        pa.field('y', pa.float32())
    ])

    v = {'x': 2, 'y': 3.5}
    s = pa.scalar(v, type=ty)
    assert s.as_py() == v
    assert s == v
    assert len(s) == 2
    assert s['x'] == 2
    assert s['y'] == 3.5

    with pytest.raises(IndexError):
        s['non-existent']


def test_map():
    ty = pa.map_(pa.string(), pa.int8())
    v = [('a', 1), ('b', 2)]
    s = pa.scalar(v, type=ty)

    assert len(s) == 2
    assert isinstance(s, pa.MapScalar)
    assert repr(s) == (
        "<pyarrow.MapScalar: ["
        "(<pyarrow.StringScalar: 'a'>, <pyarrow.Int8Scalar: 1>), "
        "(<pyarrow.StringScalar: 'b'>, <pyarrow.Int8Scalar: 2>)"
        "]>"
    )
    assert s.as_py() == v
    assert s[1] == ('b', 2)
    assert s[-1] == s[1]
    assert s[-2] == s[0]
    with pytest.raises(IndexError):
        s[-3]
    with pytest.raises(IndexError):
        s[2]


def test_dictionary():
    indices = pa.array([2, 1, 2, 0])
    dictionary = pa.array(['foo', 'bar', 'baz'])

    arr = pa.DictionaryArray.from_arrays(indices, dictionary)
    expected = ['baz', 'bar', 'baz', 'foo']

    for j, (i, v) in enumerate(zip(indices, expected)):
        s = arr[j]
        assert s.as_py() == v
        assert s.index.as_py() == i
        assert s.value.as_py() == v
        assert s.index_value == i
        assert s.dictionary_value == v


# TODO(kszucs): raise on errror signed
