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

from pyarrow.compat import unittest, u  # noqa
import pyarrow as pa

import datetime
import decimal


class TestConvertList(unittest.TestCase):

    def test_boolean(self):
        expected = [True, None, False, None]
        arr = pa.from_pylist(expected)
        assert len(arr) == 4
        assert arr.null_count == 2
        assert arr.type == pa.bool_()
        assert arr.to_pylist() == expected

    def test_empty_list(self):
        arr = pa.from_pylist([])
        assert len(arr) == 0
        assert arr.null_count == 0
        assert arr.type == pa.null()
        assert arr.to_pylist() == []

    def test_all_none(self):
        arr = pa.from_pylist([None, None])
        assert len(arr) == 2
        assert arr.null_count == 2
        assert arr.type == pa.null()
        assert arr.to_pylist() == [None, None]

    def test_integer(self):
        expected = [1, None, 3, None]
        arr = pa.from_pylist(expected)
        assert len(arr) == 4
        assert arr.null_count == 2
        assert arr.type == pa.int64()
        assert arr.to_pylist() == expected

    def test_garbage_collection(self):
        import gc

        # Force the cyclic garbage collector to run
        gc.collect()

        bytes_before = pa.total_allocated_bytes()
        pa.from_pylist([1, None, 3, None])
        gc.collect()
        assert pa.total_allocated_bytes() == bytes_before

    def test_double(self):
        data = [1.5, 1, None, 2.5, None, None]
        arr = pa.from_pylist(data)
        assert len(arr) == 6
        assert arr.null_count == 3
        assert arr.type == pa.float64()
        assert arr.to_pylist() == data

    def test_unicode(self):
        data = [u'foo', u'bar', None, u'mañana']
        arr = pa.from_pylist(data)
        assert len(arr) == 4
        assert arr.null_count == 1
        assert arr.type == pa.string()
        assert arr.to_pylist() == data

    def test_bytes(self):
        u1 = b'ma\xc3\xb1ana'
        data = [b'foo',
                u1.decode('utf-8'),  # unicode gets encoded,
                None]
        arr = pa.from_pylist(data)
        assert len(arr) == 3
        assert arr.null_count == 1
        assert arr.type == pa.binary()
        assert arr.to_pylist() == [b'foo', u1, None]

    def test_fixed_size_bytes(self):
        data = [b'foof', None, b'barb', b'2346']
        arr = pa.from_pylist(data, type=pa.binary(4))
        assert len(arr) == 4
        assert arr.null_count == 1
        assert arr.type == pa.binary(4)
        assert arr.to_pylist() == data

    def test_fixed_size_bytes_does_not_accept_varying_lengths(self):
        data = [b'foo', None, b'barb', b'2346']
        with self.assertRaises(pa.ArrowInvalid):
            pa.from_pylist(data, type=pa.binary(4))

    def test_date(self):
        data = [datetime.date(2000, 1, 1), None, datetime.date(1970, 1, 1),
                datetime.date(2040, 2, 26)]
        arr = pa.from_pylist(data)
        assert len(arr) == 4
        assert arr.type == pa.date64()
        assert arr.null_count == 1
        assert arr[0].as_py() == datetime.date(2000, 1, 1)
        assert arr[1].as_py() is None
        assert arr[2].as_py() == datetime.date(1970, 1, 1)
        assert arr[3].as_py() == datetime.date(2040, 2, 26)

    def test_timestamp(self):
        data = [
            datetime.datetime(2007, 7, 13, 1, 23, 34, 123456),
            None,
            datetime.datetime(2006, 1, 13, 12, 34, 56, 432539),
            datetime.datetime(2010, 8, 13, 5, 46, 57, 437699)
        ]
        arr = pa.from_pylist(data)
        assert len(arr) == 4
        assert arr.type == pa.timestamp('us')
        assert arr.null_count == 1
        assert arr[0].as_py() == datetime.datetime(2007, 7, 13, 1,
                                                   23, 34, 123456)
        assert arr[1].as_py() is None
        assert arr[2].as_py() == datetime.datetime(2006, 1, 13, 12,
                                                   34, 56, 432539)
        assert arr[3].as_py() == datetime.datetime(2010, 8, 13, 5,
                                                   46, 57, 437699)

    def test_mixed_nesting_levels(self):
        pa.from_pylist([1, 2, None])
        pa.from_pylist([[1], [2], None])
        pa.from_pylist([[1], [2], [None]])

        with self.assertRaises(pa.ArrowInvalid):
            pa.from_pylist([1, 2, [1]])

        with self.assertRaises(pa.ArrowInvalid):
            pa.from_pylist([1, 2, []])

        with self.assertRaises(pa.ArrowInvalid):
            pa.from_pylist([[1], [2], [None, [1]]])

    def test_list_of_int(self):
        data = [[1, 2, 3], [], None, [1, 2]]
        arr = pa.from_pylist(data)
        assert len(arr) == 4
        assert arr.null_count == 1
        assert arr.type == pa.list_(pa.int64())
        assert arr.to_pylist() == data

    def test_mixed_types_fails(self):
        data = ['a', 1, 2.0]
        with self.assertRaises(pa.ArrowException):
            pa.from_pylist(data)

    def test_decimal(self):
        data = [decimal.Decimal('1234.183'), decimal.Decimal('8094.234')]
        type = pa.decimal(precision=7, scale=3)
        arr = pa.from_pylist(data, type=type)
        assert arr.to_pylist() == data

    def test_decimal_different_precisions(self):
        data = [
            decimal.Decimal('1234234983.183'), decimal.Decimal('80943244.234')
        ]
        type = pa.decimal(precision=13, scale=3)
        arr = pa.from_pylist(data, type=type)
        assert arr.to_pylist() == data

    def test_decimal_no_scale(self):
        data = [decimal.Decimal('1234234983'), decimal.Decimal('8094324')]
        type = pa.decimal(precision=10)
        arr = pa.from_pylist(data, type=type)
        assert arr.to_pylist() == data

    def test_decimal_negative(self):
        data = [decimal.Decimal('-1234.234983'), decimal.Decimal('-8.094324')]
        type = pa.decimal(precision=10, scale=6)
        arr = pa.from_pylist(data, type=type)
        assert arr.to_pylist() == data

    def test_decimal_no_whole_part(self):
        data = [decimal.Decimal('-.4234983'), decimal.Decimal('.0103943')]
        type = pa.decimal(precision=7, scale=7)
        arr = pa.from_pylist(data, type=type)
        assert arr.to_pylist() == data

    def test_decimal_large_integer(self):
        data = [decimal.Decimal('-394029506937548693.42983'),
                decimal.Decimal('32358695912932.01033')]
        type = pa.decimal(precision=23, scale=5)
        arr = pa.from_pylist(data, type=type)
        assert arr.to_pylist() == data
