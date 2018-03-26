# -*- coding: utf-8 -*-
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

import pytest

import pandas as pd

from pyarrow.compat import unittest, u, unicode_type
import pyarrow as pa


class TestScalars(unittest.TestCase):

    def test_null_singleton(self):
        with self.assertRaises(Exception):
            pa.NAType()

    def test_ctor_null_check(self):
        # ARROW-1155
        with pytest.raises(ReferenceError):
            repr(pa.Int16Value())

        with pytest.raises(ReferenceError):
            str(pa.Int16Value())

        with pytest.raises(ReferenceError):
            repr(pa.StringValue())

    def test_bool(self):
        arr = pa.array([True, None, False, None])

        v = arr[0]
        assert isinstance(v, pa.BooleanValue)
        assert repr(v) == "True"
        assert v.as_py() is True

        assert arr[1] is pa.NA

    def test_int64(self):
        arr = pa.array([1, 2, None])

        v = arr[0]
        assert isinstance(v, pa.Int64Value)
        assert repr(v) == "1"
        assert v.as_py() == 1
        assert v == 1

        assert arr[2] is pa.NA

    def test_double(self):
        arr = pa.array([1.5, None, 3])

        v = arr[0]
        assert isinstance(v, pa.DoubleValue)
        assert repr(v) == "1.5"
        assert v.as_py() == 1.5
        assert v == 1.5

        assert arr[1] is pa.NA

        v = arr[2]
        assert v.as_py() == 3.0

    def test_string_unicode(self):
        arr = pa.array([u'foo', None, u'mañana'])

        v = arr[0]
        assert isinstance(v, pa.StringValue)
        assert v.as_py() == 'foo'
        assert v == 'foo'
        # Assert that newly created values are equal to the previously created
        # one.
        assert v == arr[0]

        assert arr[1] is pa.NA

        v = arr[2].as_py()
        assert v == u'mañana'
        assert isinstance(v, unicode_type)

    def test_bytes(self):
        arr = pa.array([b'foo', None, u('bar')])

        v = arr[0]
        assert isinstance(v, pa.BinaryValue)
        assert v.as_py() == b'foo'
        assert v == b'foo'

        assert arr[1] is pa.NA

        v = arr[2].as_py()
        assert v == b'bar'
        assert isinstance(v, bytes)

    def test_fixed_size_bytes(self):
        data = [b'foof', None, b'barb']
        arr = pa.array(data, type=pa.binary(4))

        v = arr[0]
        assert isinstance(v, pa.FixedSizeBinaryValue)
        assert v.as_py() == b'foof'

        assert arr[1] is pa.NA

        v = arr[2].as_py()
        assert v == b'barb'
        assert isinstance(v, bytes)

    def test_list(self):
        arr = pa.array([['foo', None], None, ['bar'], []])

        v = arr[0]
        assert len(v) == 2
        assert isinstance(v, pa.ListValue)
        assert repr(v) == "['foo', None]"
        assert v.as_py() == ['foo', None]
        assert v[0].as_py() == 'foo'
        assert v[1] is pa.NA
        assert v[-1] == v[1]
        assert v[-2] == v[0]
        with pytest.raises(IndexError):
            v[-3]
        with pytest.raises(IndexError):
            v[2]

        assert arr[1] is pa.NA

        v = arr[3]
        assert len(v) == 0

    def test_timestamp(self):
        arr = pd.date_range('2000-01-01 12:34:56', periods=10).values

        units = ['s', 'ms', 'us', 'ns']

        for unit in units:
            dtype = 'datetime64[{0}]'.format(unit)
            arrow_arr = pa.Array.from_pandas(arr.astype(dtype))
            expected = pd.Timestamp('2000-01-01 12:34:56')

            assert arrow_arr[0].as_py() == expected

            tz = 'America/New_York'
            arrow_type = pa.timestamp(unit, tz=tz)

            dtype = 'datetime64[{0}]'.format(unit)
            arrow_arr = pa.Array.from_pandas(arr.astype(dtype),
                                             type=arrow_type)
            expected = (pd.Timestamp('2000-01-01 12:34:56')
                        .tz_localize('utc')
                        .tz_convert(tz))

            assert arrow_arr[0].as_py() == expected

    def test_dictionary(self):
        colors = ['red', 'green', 'blue']
        values = pd.Series(colors * 4)

        categorical = pd.Categorical(values, categories=colors)

        v = pa.DictionaryArray.from_arrays(categorical.codes,
                                           categorical.categories)
        for i, c in enumerate(values):
            assert v[i].as_py() == c

    def test_int_hash(self):
        # ARROW-640
        int_arr = pa.array([1, 1, 2, 1])
        assert hash(int_arr[0]) == hash(1)

    def test_float_hash(self):
        # ARROW-640
        float_arr = pa.array([1.4, 1.2, 2.5, 1.8])
        assert hash(float_arr[0]) == hash(1.4)

    def test_string_hash(self):
        # ARROW-640
        str_arr = pa.array(["foo", "bar"])
        assert hash(str_arr[1]) == hash("bar")

    def test_bytes_hash(self):
        # ARROW-640
        byte_arr = pa.array([b'foo', None, b'bar'])
        assert hash(byte_arr[2]) == hash(b"bar")

    def test_array_to_set(self):
        # ARROW-640
        arr = pa.array([1, 1, 2, 1])
        set_from_array = set(arr)
        assert isinstance(set_from_array, set)
        assert set_from_array == {1, 2}
