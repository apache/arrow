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

from collections import OrderedDict

from datetime import date, time
import unittest
import decimal
import json

import pytest

import numpy as np
import numpy.testing as npt

import pandas as pd
import pandas.util.testing as tm

from pyarrow.compat import u
import pyarrow as pa

from .pandas_examples import dataframe_with_arrays, dataframe_with_lists


def _alltypes_example(size=100):
    return pd.DataFrame({
        'uint8': np.arange(size, dtype=np.uint8),
        'uint16': np.arange(size, dtype=np.uint16),
        'uint32': np.arange(size, dtype=np.uint32),
        'uint64': np.arange(size, dtype=np.uint64),
        'int8': np.arange(size, dtype=np.int16),
        'int16': np.arange(size, dtype=np.int16),
        'int32': np.arange(size, dtype=np.int32),
        'int64': np.arange(size, dtype=np.int64),
        'float32': np.arange(size, dtype=np.float32),
        'float64': np.arange(size, dtype=np.float64),
        'bool': np.random.randn(size) > 0,
        # TODO(wesm): Pandas only support ns resolution, Arrow supports s, ms,
        # us, ns
        'datetime': np.arange("2016-01-01T00:00:00.001", size,
                              dtype='datetime64[ms]'),
        'str': [str(x) for x in range(size)],
        'str_with_nulls': [None] + [str(x) for x in range(size - 2)] + [None],
        'empty_str': [''] * size
    })


class TestPandasConversion(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def _check_pandas_roundtrip(self, df, expected=None, nthreads=1,
                                expected_schema=None,
                                check_dtype=True, schema=None,
                                check_index=False):
        table = pa.Table.from_pandas(df,
                                     schema=schema, preserve_index=check_index)
        result = table.to_pandas(nthreads=nthreads)
        if expected_schema:
            assert table.schema.equals(expected_schema)
        if expected is None:
            expected = df
        tm.assert_frame_equal(result, expected, check_dtype=check_dtype)

    def _check_series_roundtrip(self, s, type_=None):
        arr = pa.array(s, from_pandas=True, type=type_)

        result = pd.Series(arr.to_pandas(), name=s.name)
        if isinstance(arr.type, pa.TimestampType) and arr.type.tz is not None:
            result = (result.dt.tz_localize('utc')
                      .dt.tz_convert(arr.type.tz))

        tm.assert_series_equal(s, result)

    def _check_array_roundtrip(self, values, expected=None, mask=None,
                               type=None):
        arr = pa.array(values, from_pandas=True, mask=mask, type=type)
        result = arr.to_pandas()

        values_nulls = pd.isnull(values)
        if mask is None:
            assert arr.null_count == values_nulls.sum()
        else:
            assert arr.null_count == (mask | values_nulls).sum()

        if mask is None:
            tm.assert_series_equal(pd.Series(result), pd.Series(values),
                                   check_names=False)
        else:
            expected = pd.Series(np.ma.masked_array(values, mask=mask))
            tm.assert_series_equal(pd.Series(result), expected,
                                   check_names=False)

    def test_all_none_objects(self):
        df = pd.DataFrame({'a': [None, None, None]})
        self._check_pandas_roundtrip(df)

    def test_all_none_category(self):
        df = pd.DataFrame({'a': [None, None, None]})
        df['a'] = df['a'].astype('category')
        self._check_pandas_roundtrip(df)

    def test_non_string_columns(self):
        df = pd.DataFrame({0: [1, 2, 3]})
        table = pa.Table.from_pandas(df)
        assert table.column(0).name == '0'

    def test_float_no_nulls(self):
        data = {}
        fields = []
        dtypes = [('f4', pa.float32()), ('f8', pa.float64())]
        num_values = 100

        for numpy_dtype, arrow_dtype in dtypes:
            values = np.random.randn(num_values)
            data[numpy_dtype] = values.astype(numpy_dtype)
            fields.append(pa.field(numpy_dtype, arrow_dtype))

        df = pd.DataFrame(data)
        schema = pa.schema(fields)
        self._check_pandas_roundtrip(df, expected_schema=schema)

    def test_float_nulls(self):
        num_values = 100

        null_mask = np.random.randint(0, 10, size=num_values) < 3
        dtypes = [('f4', pa.float32()), ('f8', pa.float64())]
        names = ['f4', 'f8']
        expected_cols = []

        arrays = []
        fields = []
        for name, arrow_dtype in dtypes:
            values = np.random.randn(num_values).astype(name)

            arr = pa.array(values, from_pandas=True, mask=null_mask)
            arrays.append(arr)
            fields.append(pa.field(name, arrow_dtype))
            values[null_mask] = np.nan

            expected_cols.append(values)

        ex_frame = pd.DataFrame(dict(zip(names, expected_cols)),
                                columns=names)

        table = pa.Table.from_arrays(arrays, names)
        assert table.schema.equals(pa.schema(fields))
        result = table.to_pandas()
        tm.assert_frame_equal(result, ex_frame)

    def test_float_object_nulls(self):
        arr = np.array([None, 1.5, np.float64(3.5)] * 5, dtype=object)
        df = pd.DataFrame({'floats': arr})
        expected = pd.DataFrame({'floats': pd.to_numeric(arr)})
        field = pa.field('floats', pa.float64())
        schema = pa.schema([field])
        self._check_pandas_roundtrip(df, expected=expected,
                                     expected_schema=schema)

    def test_int_object_nulls(self):
        arr = np.array([None, 1, np.int64(3)] * 5, dtype=object)
        df = pd.DataFrame({'ints': arr})
        expected = pd.DataFrame({'ints': pd.to_numeric(arr)})
        field = pa.field('ints', pa.int64())
        schema = pa.schema([field])
        self._check_pandas_roundtrip(df, expected=expected,
                                     expected_schema=schema)

    def test_integer_no_nulls(self):
        data = OrderedDict()
        fields = []

        numpy_dtypes = [
            ('i1', pa.int8()), ('i2', pa.int16()),
            ('i4', pa.int32()), ('i8', pa.int64()),
            ('u1', pa.uint8()), ('u2', pa.uint16()),
            ('u4', pa.uint32()), ('u8', pa.uint64()),
            ('longlong', pa.int64()), ('ulonglong', pa.uint64())
        ]
        num_values = 100

        for dtype, arrow_dtype in numpy_dtypes:
            info = np.iinfo(dtype)
            values = np.random.randint(max(info.min, np.iinfo(np.int_).min),
                                       min(info.max, np.iinfo(np.int_).max),
                                       size=num_values)
            data[dtype] = values.astype(dtype)
            fields.append(pa.field(dtype, arrow_dtype))

        df = pd.DataFrame(data)
        schema = pa.schema(fields)
        self._check_pandas_roundtrip(df, expected_schema=schema)

    def test_integer_with_nulls(self):
        # pandas requires upcast to float dtype

        int_dtypes = ['i1', 'i2', 'i4', 'i8', 'u1', 'u2', 'u4', 'u8']
        num_values = 100

        null_mask = np.random.randint(0, 10, size=num_values) < 3

        expected_cols = []
        arrays = []
        for name in int_dtypes:
            values = np.random.randint(0, 100, size=num_values)

            arr = pa.array(values, mask=null_mask)
            arrays.append(arr)

            expected = values.astype('f8')
            expected[null_mask] = np.nan

            expected_cols.append(expected)

        ex_frame = pd.DataFrame(dict(zip(int_dtypes, expected_cols)),
                                columns=int_dtypes)

        table = pa.Table.from_arrays(arrays, int_dtypes)
        result = table.to_pandas()

        tm.assert_frame_equal(result, ex_frame)

    def test_array_from_pandas_type_cast(self):
        arr = np.arange(10, dtype='int64')

        target_type = pa.int8()

        result = pa.array(arr, type=target_type)
        expected = pa.array(arr.astype('int8'))
        assert result.equals(expected)

    def test_boolean_no_nulls(self):
        num_values = 100

        np.random.seed(0)

        df = pd.DataFrame({'bools': np.random.randn(num_values) > 0})
        field = pa.field('bools', pa.bool_())
        schema = pa.schema([field])
        self._check_pandas_roundtrip(df, expected_schema=schema)

    def test_boolean_nulls(self):
        # pandas requires upcast to object dtype
        num_values = 100
        np.random.seed(0)

        mask = np.random.randint(0, 10, size=num_values) < 3
        values = np.random.randint(0, 10, size=num_values) < 5

        arr = pa.array(values, mask=mask)

        expected = values.astype(object)
        expected[mask] = None

        field = pa.field('bools', pa.bool_())
        schema = pa.schema([field])
        ex_frame = pd.DataFrame({'bools': expected})

        table = pa.Table.from_arrays([arr], ['bools'])
        assert table.schema.equals(schema)
        result = table.to_pandas()

        tm.assert_frame_equal(result, ex_frame)

    def test_boolean_object_nulls(self):
        arr = np.array([False, None, True] * 100, dtype=object)
        df = pd.DataFrame({'bools': arr})
        field = pa.field('bools', pa.bool_())
        schema = pa.schema([field])
        self._check_pandas_roundtrip(df, expected_schema=schema)

    def test_all_nulls_cast_numeric(self):
        arr = np.array([None], dtype=object)

        def _check_type(t):
            a2 = pa.array(arr, type=t)
            assert a2.type == t
            assert a2[0].as_py() is None

        _check_type(pa.int32())
        _check_type(pa.float64())

    def test_unicode(self):
        repeats = 1000
        values = [u'foo', None, u'bar', u'mañana', np.nan]
        df = pd.DataFrame({'strings': values * repeats})
        field = pa.field('strings', pa.string())
        schema = pa.schema([field])

        self._check_pandas_roundtrip(df, expected_schema=schema)

    def test_bytes_to_binary(self):
        values = [u('qux'), b'foo', None, 'bar', 'qux', np.nan]
        df = pd.DataFrame({'strings': values})

        table = pa.Table.from_pandas(df)
        assert table[0].type == pa.binary()

        values2 = [b'qux', b'foo', None, b'bar', b'qux', np.nan]
        expected = pd.DataFrame({'strings': values2})
        self._check_pandas_roundtrip(df, expected)

    @pytest.mark.large_memory
    def test_bytes_exceed_2gb(self):
        val = 'x' * (1 << 20)
        df = pd.DataFrame({
            'strings': np.array([val] * 4000, dtype=object)
        })
        arr = pa.array(df['strings'])
        assert isinstance(arr, pa.ChunkedArray)
        assert arr.num_chunks == 2
        arr = None

        table = pa.Table.from_pandas(df)
        assert table[0].data.num_chunks == 2

    def test_fixed_size_bytes(self):
        values = [b'foo', None, b'bar', None, None, b'hey']
        df = pd.DataFrame({'strings': values})
        schema = pa.schema([pa.field('strings', pa.binary(3))])
        table = pa.Table.from_pandas(df, schema=schema)
        assert table.schema[0].type == schema[0].type
        assert table.schema[0].name == schema[0].name
        result = table.to_pandas()
        tm.assert_frame_equal(result, df)

    def test_fixed_size_bytes_does_not_accept_varying_lengths(self):
        values = [b'foo', None, b'ba', None, None, b'hey']
        df = pd.DataFrame({'strings': values})
        schema = pa.schema([pa.field('strings', pa.binary(3))])
        with self.assertRaises(pa.ArrowInvalid):
            pa.Table.from_pandas(df, schema=schema)

    def test_timestamps_notimezone_no_nulls(self):
        df = pd.DataFrame({
            'datetime64': np.array([
                '2007-07-13T01:23:34.123456789',
                '2006-01-13T12:34:56.432539784',
                '2010-08-13T05:46:57.437699912'],
                dtype='datetime64[ns]')
        })
        field = pa.field('datetime64', pa.timestamp('ns'))
        schema = pa.schema([field])
        self._check_pandas_roundtrip(
            df,
            expected_schema=schema,
        )

    def test_timestamps_notimezone_nulls(self):
        df = pd.DataFrame({
            'datetime64': np.array([
                '2007-07-13T01:23:34.123456789',
                None,
                '2010-08-13T05:46:57.437699912'],
                dtype='datetime64[ns]')
        })
        field = pa.field('datetime64', pa.timestamp('ns'))
        schema = pa.schema([field])
        self._check_pandas_roundtrip(
            df,
            expected_schema=schema,
        )

    def test_timestamps_with_timezone(self):
        df = pd.DataFrame({
            'datetime64': np.array([
                '2007-07-13T01:23:34.123',
                '2006-01-13T12:34:56.432',
                '2010-08-13T05:46:57.437'],
                dtype='datetime64[ms]')
        })
        df['datetime64'] = (df['datetime64'].dt.tz_localize('US/Eastern')
                            .to_frame())
        self._check_pandas_roundtrip(df)

        self._check_series_roundtrip(df['datetime64'])

        # drop-in a null and ns instead of ms
        df = pd.DataFrame({
            'datetime64': np.array([
                '2007-07-13T01:23:34.123456789',
                None,
                '2006-01-13T12:34:56.432539784',
                '2010-08-13T05:46:57.437699912'],
                dtype='datetime64[ns]')
        })
        df['datetime64'] = (df['datetime64'].dt.tz_localize('US/Eastern')
                            .to_frame())

        self._check_pandas_roundtrip(df)

    def test_date_infer(self):
        df = pd.DataFrame({
            'date': [date(2000, 1, 1),
                     None,
                     date(1970, 1, 1),
                     date(2040, 2, 26)]})
        table = pa.Table.from_pandas(df, preserve_index=False)
        field = pa.field('date', pa.date32())
        schema = pa.schema([field])
        assert table.schema.equals(schema)
        result = table.to_pandas()
        expected = df.copy()
        expected['date'] = pd.to_datetime(df['date'])
        tm.assert_frame_equal(result, expected)

    def test_date_objects_typed(self):
        arr = np.array([
            date(2017, 4, 3),
            None,
            date(2017, 4, 4),
            date(2017, 4, 5)], dtype=object)

        arr_i4 = np.array([17259, -1, 17260, 17261], dtype='int32')
        arr_i8 = arr_i4.astype('int64') * 86400000
        mask = np.array([False, True, False, False])

        t32 = pa.date32()
        t64 = pa.date64()

        a32 = pa.array(arr, type=t32)
        a64 = pa.array(arr, type=t64)

        a32_expected = pa.array(arr_i4, mask=mask, type=t32)
        a64_expected = pa.array(arr_i8, mask=mask, type=t64)

        assert a32.equals(a32_expected)
        assert a64.equals(a64_expected)

        # Test converting back to pandas
        colnames = ['date32', 'date64']
        table = pa.Table.from_arrays([a32, a64], colnames)
        table_pandas = table.to_pandas()

        ex_values = (np.array(['2017-04-03', '2017-04-04', '2017-04-04',
                               '2017-04-05'],
                              dtype='datetime64[D]')
                     .astype('datetime64[ns]'))
        ex_values[1] = pd.NaT.value
        expected_pandas = pd.DataFrame({'date32': ex_values,
                                        'date64': ex_values},
                                       columns=colnames)
        tm.assert_frame_equal(table_pandas, expected_pandas)

    def test_dates_from_integers(self):
        t1 = pa.date32()
        t2 = pa.date64()

        arr = np.array([17259, 17260, 17261], dtype='int32')
        arr2 = arr.astype('int64') * 86400000

        a1 = pa.array(arr, type=t1)
        a2 = pa.array(arr2, type=t2)

        expected = date(2017, 4, 3)
        assert a1[0].as_py() == expected
        assert a2[0].as_py() == expected

    @pytest.mark.xfail(reason="not supported ATM",
                       raises=NotImplementedError)
    def test_timedelta(self):
        # TODO(jreback): Pandas only support ns resolution
        # Arrow supports ??? for resolution
        df = pd.DataFrame({
            'timedelta': np.arange(start=0, stop=3 * 86400000,
                                   step=86400000,
                                   dtype='timedelta64[ms]')
        })
        pa.Table.from_pandas(df)

    def test_column_of_arrays(self):
        df, schema = dataframe_with_arrays()
        self._check_pandas_roundtrip(df, schema=schema, expected_schema=schema)
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        assert table.schema.equals(schema)

        for column in df.columns:
            field = schema.field_by_name(column)
            self._check_array_roundtrip(df[column], type=field.type)

    def test_column_of_arrays_to_py(self):
        # Test regression in ARROW-1199 not caught in above test
        dtype = 'i1'
        arr = np.array([
            np.arange(10, dtype=dtype),
            np.arange(5, dtype=dtype),
            None,
            np.arange(1, dtype=dtype)
        ])
        type_ = pa.list_(pa.int8())
        parr = pa.array(arr, type=type_)

        assert parr[0].as_py() == list(range(10))
        assert parr[1].as_py() == list(range(5))
        assert parr[2].as_py() is None
        assert parr[3].as_py() == [0]

    def test_column_of_lists(self):
        df, schema = dataframe_with_lists()
        self._check_pandas_roundtrip(df, schema=schema, expected_schema=schema)
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        assert table.schema.equals(schema)

        for column in df.columns:
            field = schema.field_by_name(column)
            self._check_array_roundtrip(df[column], type=field.type)

    def test_column_of_lists_chunked(self):
        # ARROW-1357
        df = pd.DataFrame({
            'lists': np.array([
                [1, 2],
                None,
                [2, 3],
                [4, 5],
                [6, 7],
                [8, 9]
            ], dtype=object)
        })

        schema = pa.schema([
            pa.field('lists', pa.list_(pa.int64()))
        ])

        t1 = pa.Table.from_pandas(df[:2], schema=schema)
        t2 = pa.Table.from_pandas(df[2:], schema=schema)

        table = pa.concat_tables([t1, t2])
        result = table.to_pandas()

        tm.assert_frame_equal(result, df)

    def test_column_of_lists_chunked2(self):
        data1 = [[0, 1], [2, 3], [4, 5], [6, 7], [10, 11],
                 [12, 13], [14, 15], [16, 17]]
        data2 = [[8, 9], [18, 19]]

        a1 = pa.array(data1)
        a2 = pa.array(data2)

        t1 = pa.Table.from_arrays([a1], names=['a'])
        t2 = pa.Table.from_arrays([a2], names=['a'])

        concatenated = pa.concat_tables([t1, t2])

        result = concatenated.to_pandas()
        expected = pd.DataFrame({'a': data1 + data2})

        tm.assert_frame_equal(result, expected)

    def test_column_of_lists_strided(self):
        df, schema = dataframe_with_lists()
        df = pd.concat([df] * 6, ignore_index=True)

        arr = df['int64'].values[::3]
        assert arr.strides[0] != 8

        self._check_array_roundtrip(arr)

    def test_nested_lists_all_none(self):
        data = np.array([[None, None], None], dtype=object)

        arr = pa.array(data)
        expected = pa.array(list(data))
        assert arr.equals(expected)
        assert arr.type == pa.list_(pa.null())

        data2 = np.array([None, None, [None, None],
                          np.array([None, None], dtype=object)],
                         dtype=object)
        arr = pa.array(data2)
        expected = pa.array([None, None, [None, None], [None, None]])
        assert arr.equals(expected)

    def test_threaded_conversion(self):
        df = _alltypes_example()
        self._check_pandas_roundtrip(df, nthreads=2)

    def test_category(self):
        repeats = 5
        v1 = ['foo', None, 'bar', 'qux', np.nan]
        v2 = [4, 5, 6, 7, 8]
        v3 = [b'foo', None, b'bar', b'qux', np.nan]
        df = pd.DataFrame({'cat_strings': pd.Categorical(v1 * repeats),
                           'cat_ints': pd.Categorical(v2 * repeats),
                           'cat_binary': pd.Categorical(v3 * repeats),
                           'cat_strings_ordered': pd.Categorical(
                               v1 * repeats, categories=['bar', 'qux', 'foo'],
                               ordered=True),
                           'ints': v2 * repeats,
                           'ints2': v2 * repeats,
                           'strings': v1 * repeats,
                           'strings2': v1 * repeats,
                           'strings3': v3 * repeats})
        self._check_pandas_roundtrip(df)

        arrays = [
            pd.Categorical(v1 * repeats),
            pd.Categorical(v2 * repeats),
            pd.Categorical(v3 * repeats)
        ]
        for values in arrays:
            self._check_array_roundtrip(values)

    def test_mixed_types_fails(self):
        data = pd.DataFrame({'a': ['a', 1, 2.0]})
        with self.assertRaises(pa.ArrowException):
            pa.Table.from_pandas(data)

        data = pd.DataFrame({'a': [1, True]})
        with self.assertRaises(pa.ArrowException):
            pa.Table.from_pandas(data)

    def test_strided_data_import(self):
        cases = []

        columns = ['a', 'b', 'c']
        N, K = 100, 3
        random_numbers = np.random.randn(N, K).copy() * 100

        numeric_dtypes = ['i1', 'i2', 'i4', 'i8', 'u1', 'u2', 'u4', 'u8',
                          'f4', 'f8']

        for type_name in numeric_dtypes:
            cases.append(random_numbers.astype(type_name))

        # strings
        cases.append(np.array([tm.rands(10) for i in range(N * K)],
                              dtype=object)
                     .reshape(N, K).copy())

        # booleans
        boolean_objects = (np.array([True, False, True] * N, dtype=object)
                           .reshape(N, K).copy())

        # add some nulls, so dtype comes back as objects
        boolean_objects[5] = None
        cases.append(boolean_objects)

        cases.append(np.arange("2016-01-01T00:00:00.001", N * K,
                               dtype='datetime64[ms]')
                     .reshape(N, K).copy())

        strided_mask = (random_numbers > 0).astype(bool)[:, 0]

        for case in cases:
            df = pd.DataFrame(case, columns=columns)
            col = df['a']

            self._check_pandas_roundtrip(df)
            self._check_array_roundtrip(col)
            self._check_array_roundtrip(col, mask=strided_mask)

    def test_decimal_32_from_pandas(self):
        expected = pd.DataFrame({
            'decimals': [
                decimal.Decimal('-1234.123'),
                decimal.Decimal('1234.439'),
            ]
        })
        converted = pa.Table.from_pandas(expected, preserve_index=False)
        field = pa.field('decimals', pa.decimal(7, 3))
        schema = pa.schema([field])
        assert converted.schema.equals(schema)

    def test_decimal_32_to_pandas(self):
        expected = pd.DataFrame({
            'decimals': [
                decimal.Decimal('-1234.123'),
                decimal.Decimal('1234.439'),
            ]
        })
        converted = pa.Table.from_pandas(expected)
        df = converted.to_pandas()
        tm.assert_frame_equal(df, expected)

    def test_decimal_64_from_pandas(self):
        expected = pd.DataFrame({
            'decimals': [
                decimal.Decimal('-129934.123331'),
                decimal.Decimal('129534.123731'),
            ]
        })
        converted = pa.Table.from_pandas(expected, preserve_index=False)
        field = pa.field('decimals', pa.decimal(12, 6))
        schema = pa.schema([field])
        assert converted.schema.equals(schema)

    def test_decimal_64_to_pandas(self):
        expected = pd.DataFrame({
            'decimals': [
                decimal.Decimal('-129934.123331'),
                decimal.Decimal('129534.123731'),
            ]
        })
        converted = pa.Table.from_pandas(expected)
        df = converted.to_pandas()
        tm.assert_frame_equal(df, expected)

    def test_decimal_128_from_pandas(self):
        expected = pd.DataFrame({
            'decimals': [
                decimal.Decimal('394092382910493.12341234678'),
                -decimal.Decimal('314292388910493.12343437128'),
            ]
        })
        converted = pa.Table.from_pandas(expected, preserve_index=False)
        field = pa.field('decimals', pa.decimal(26, 11))
        schema = pa.schema([field])
        assert converted.schema.equals(schema)

    def test_decimal_128_to_pandas(self):
        expected = pd.DataFrame({
            'decimals': [
                decimal.Decimal('394092382910493.12341234678'),
                -decimal.Decimal('314292388910493.12343437128'),
            ]
        })
        converted = pa.Table.from_pandas(expected)
        df = converted.to_pandas()
        tm.assert_frame_equal(df, expected)

    def test_pytime_from_pandas(self):
        pytimes = [time(1, 2, 3, 1356),
                   time(4, 5, 6, 1356)]

        # microseconds
        t1 = pa.time64('us')

        aobjs = np.array(pytimes + [None], dtype=object)
        parr = pa.array(aobjs)
        assert parr.type == t1
        assert parr[0].as_py() == pytimes[0]
        assert parr[1].as_py() == pytimes[1]
        assert parr[2] is pa.NA

        # DataFrame
        df = pd.DataFrame({'times': aobjs})
        batch = pa.RecordBatch.from_pandas(df)
        assert batch[0].equals(parr)

        # Test ndarray of int64 values
        arr = np.array([_pytime_to_micros(v) for v in pytimes],
                       dtype='int64')

        a1 = pa.array(arr, type=pa.time64('us'))
        assert a1[0].as_py() == pytimes[0]

        a2 = pa.array(arr * 1000, type=pa.time64('ns'))
        assert a2[0].as_py() == pytimes[0]

        a3 = pa.array((arr / 1000).astype('i4'),
                      type=pa.time32('ms'))
        assert a3[0].as_py() == pytimes[0].replace(microsecond=1000)

        a4 = pa.array((arr / 1000000).astype('i4'),
                      type=pa.time32('s'))
        assert a4[0].as_py() == pytimes[0].replace(microsecond=0)

    def test_arrow_time_to_pandas(self):
        pytimes = [time(1, 2, 3, 1356),
                   time(4, 5, 6, 1356),
                   time(0, 0, 0)]

        expected = np.array(pytimes[:2] + [None])
        expected_ms = np.array([x.replace(microsecond=1000)
                                for x in pytimes[:2]] +
                               [None])
        expected_s = np.array([x.replace(microsecond=0)
                               for x in pytimes[:2]] +
                              [None])

        arr = np.array([_pytime_to_micros(v) for v in pytimes],
                       dtype='int64')
        arr = np.array([_pytime_to_micros(v) for v in pytimes],
                       dtype='int64')

        null_mask = np.array([False, False, True], dtype=bool)

        a1 = pa.array(arr, mask=null_mask, type=pa.time64('us'))
        a2 = pa.array(arr * 1000, mask=null_mask,
                      type=pa.time64('ns'))

        a3 = pa.array((arr / 1000).astype('i4'), mask=null_mask,
                      type=pa.time32('ms'))
        a4 = pa.array((arr / 1000000).astype('i4'), mask=null_mask,
                      type=pa.time32('s'))

        names = ['time64[us]', 'time64[ns]', 'time32[ms]', 'time32[s]']
        batch = pa.RecordBatch.from_arrays([a1, a2, a3, a4], names)
        arr = a1.to_pandas()
        assert (arr == expected).all()

        arr = a2.to_pandas()
        assert (arr == expected).all()

        arr = a3.to_pandas()
        assert (arr == expected_ms).all()

        arr = a4.to_pandas()
        assert (arr == expected_s).all()

        df = batch.to_pandas()
        expected_df = pd.DataFrame({'time64[us]': expected,
                                    'time64[ns]': expected,
                                    'time32[ms]': expected_ms,
                                    'time32[s]': expected_s},
                                   columns=names)

        tm.assert_frame_equal(df, expected_df)

    def _check_array_from_pandas_roundtrip(self, np_array):
        arr = pa.array(np_array, from_pandas=True)
        result = arr.to_pandas()
        npt.assert_array_equal(result, np_array)

    def test_numpy_datetime64_columns(self):
        datetime64_ns = np.array([
                '2007-07-13T01:23:34.123456789',
                None,
                '2006-01-13T12:34:56.432539784',
                '2010-08-13T05:46:57.437699912'],
                dtype='datetime64[ns]')
        self._check_array_from_pandas_roundtrip(datetime64_ns)

        datetime64_us = np.array([
                '2007-07-13T01:23:34.123456',
                None,
                '2006-01-13T12:34:56.432539',
                '2010-08-13T05:46:57.437699'],
                dtype='datetime64[us]')
        self._check_array_from_pandas_roundtrip(datetime64_us)

        datetime64_ms = np.array([
                '2007-07-13T01:23:34.123',
                None,
                '2006-01-13T12:34:56.432',
                '2010-08-13T05:46:57.437'],
                dtype='datetime64[ms]')
        self._check_array_from_pandas_roundtrip(datetime64_ms)

        datetime64_s = np.array([
                '2007-07-13T01:23:34',
                None,
                '2006-01-13T12:34:56',
                '2010-08-13T05:46:57'],
                dtype='datetime64[s]')
        self._check_array_from_pandas_roundtrip(datetime64_s)

        datetime64_d = np.array([
                '2007-07-13',
                None,
                '2006-01-15',
                '2010-08-19'],
                dtype='datetime64[D]')
        self._check_array_from_pandas_roundtrip(datetime64_d)

    def test_all_nones(self):
        def _check_series(s):
            converted = pa.array(s)
            assert isinstance(converted, pa.NullArray)
            assert len(converted) == 3
            assert converted.null_count == 3
            assert converted[0] is pa.NA

        _check_series(pd.Series([None] * 3, dtype=object))
        _check_series(pd.Series([np.nan] * 3, dtype=object))
        _check_series(pd.Series([np.sqrt(-1)] * 3, dtype=object))

    def test_multiindex_duplicate_values(self):
        num_rows = 3
        numbers = list(range(num_rows))
        index = pd.MultiIndex.from_arrays(
            [['foo', 'foo', 'bar'], numbers],
            names=['foobar', 'some_numbers'],
        )

        df = pd.DataFrame({'numbers': numbers}, index=index)

        table = pa.Table.from_pandas(df)
        result_df = table.to_pandas()
        tm.assert_frame_equal(result_df, df)

    def test_partial_schema(self):
        data = OrderedDict([
            ('a', [0, 1, 2, 3, 4]),
            ('b', np.array([-10, -5, 0, 5, 10], dtype=np.int32)),
            ('c', [-10, -5, 0, 5, 10])
        ])
        df = pd.DataFrame(data)

        partial_schema = pa.schema([
            pa.field('a', pa.int64()),
            pa.field('b', pa.int32())
        ])

        expected_schema = pa.schema([
            pa.field('a', pa.int64()),
            pa.field('b', pa.int32()),
            pa.field('c', pa.int64())
        ])

        self._check_pandas_roundtrip(df, schema=partial_schema,
                                     expected_schema=expected_schema)

    def test_structarray(self):
        ints = pa.array([None, 2, 3], type=pa.int64())
        strs = pa.array([u'a', None, u'c'], type=pa.string())
        bools = pa.array([True, False, None], type=pa.bool_())
        arr = pa.StructArray.from_arrays(
            ['ints', 'strs', 'bools'],
            [ints, strs, bools])

        expected = pd.Series([
            {'ints': None, 'strs': u'a', 'bools': True},
            {'ints': 2, 'strs': None, 'bools': False},
            {'ints': 3, 'strs': u'c', 'bools': None},
        ])

        series = pd.Series(arr.to_pandas())
        tm.assert_series_equal(series, expected)

    def test_infer_lists(self):
        data = OrderedDict([
            ('nan_ints', [[None, 1], [2, 3]]),
            ('ints', [[0, 1], [2, 3]]),
            ('strs', [[None, u'b'], [u'c', u'd']]),
            ('nested_strs', [[[None, u'b'], [u'c', u'd']], None])
        ])
        df = pd.DataFrame(data)

        expected_schema = pa.schema([
            pa.field('nan_ints', pa.list_(pa.int64())),
            pa.field('ints', pa.list_(pa.int64())),
            pa.field('strs', pa.list_(pa.string())),
            pa.field('nested_strs', pa.list_(pa.list_(pa.string())))
        ])

        self._check_pandas_roundtrip(df, expected_schema=expected_schema)

    def test_infer_numpy_array(self):
        data = OrderedDict([
            ('ints', [
                np.array([0, 1], dtype=np.int64),
                np.array([2, 3], dtype=np.int64)
            ])
        ])
        df = pd.DataFrame(data)
        expected_schema = pa.schema([
            pa.field('ints', pa.list_(pa.int64()))
        ])

        self._check_pandas_roundtrip(df, expected_schema=expected_schema)

    def test_metadata_with_mixed_types(self):
        df = pd.DataFrame({'data': [b'some_bytes', u'some_unicode']})
        table = pa.Table.from_pandas(df)
        metadata = table.schema.metadata
        assert b'mixed' not in metadata[b'pandas']

        js = json.loads(metadata[b'pandas'].decode('utf8'))
        data_column = js['columns'][0]
        assert data_column['pandas_type'] == 'bytes'
        assert data_column['numpy_type'] == 'object'

    def test_list_metadata(self):
        df = pd.DataFrame({'data': [[1], [2, 3, 4], [5] * 7]})
        schema = pa.schema([pa.field('data', type=pa.list_(pa.int64()))])
        table = pa.Table.from_pandas(df, schema=schema)
        metadata = table.schema.metadata
        assert b'mixed' not in metadata[b'pandas']

        js = json.loads(metadata[b'pandas'].decode('utf8'))
        data_column = js['columns'][0]
        assert data_column['pandas_type'] == 'list[int64]'
        assert data_column['numpy_type'] == 'object'

    def test_decimal_metadata(self):
        expected = pd.DataFrame({
            'decimals': [
                decimal.Decimal('394092382910493.12341234678'),
                -decimal.Decimal('314292388910493.12343437128'),
            ]
        })
        table = pa.Table.from_pandas(expected)
        metadata = table.schema.metadata
        assert b'mixed' not in metadata[b'pandas']

        js = json.loads(metadata[b'pandas'].decode('utf8'))
        data_column = js['columns'][0]
        assert data_column['pandas_type'] == 'decimal'
        assert data_column['numpy_type'] == 'object'
        assert data_column['metadata'] == {'precision': 26, 'scale': 11}

    def test_table_str_to_categorical(self):
        values = [None, 'a', 'b', np.nan]
        df = pd.DataFrame({'strings': values})
        field = pa.field('strings', pa.string())
        schema = pa.schema([field])
        table = pa.Table.from_pandas(df, schema=schema)

        result = table.to_pandas(strings_to_categorical=True)
        expected = pd.DataFrame({'strings': pd.Categorical(values)})
        tm.assert_frame_equal(result, expected, check_dtype=True)


def _pytime_from_micros(val):
    microseconds = val % 1000000
    val //= 1000000
    seconds = val % 60
    val //= 60
    minutes = val % 60
    hours = val // 60
    return time(hours, minutes, seconds, microseconds)


def _pytime_to_micros(pytime):
    return (pytime.hour * 3600000000 +
            pytime.minute * 60000000 +
            pytime.second * 1000000 +
            pytime.microsecond)
