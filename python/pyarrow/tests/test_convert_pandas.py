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

import datetime
import unittest

import numpy as np

import pandas as pd
import pandas.util.testing as tm

from pyarrow.compat import u
import pyarrow as A

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
                                timestamps_to_ms=False, expected_schema=None,
                                check_dtype=True, schema=None):
        table = A.Table.from_pandas(df, timestamps_to_ms=timestamps_to_ms,
                                    schema=schema)
        result = table.to_pandas(nthreads=nthreads)
        if expected_schema:
            assert table.schema.equals(expected_schema)
        if expected is None:
            expected = df
        tm.assert_frame_equal(result, expected, check_dtype=check_dtype)

    def _check_array_roundtrip(self, values, expected=None, mask=None,
                               timestamps_to_ms=False, type=None):
        arr = A.Array.from_numpy(values, timestamps_to_ms=timestamps_to_ms,
                                 mask=mask, type=type)
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

    def test_float_no_nulls(self):
        data = {}
        fields = []
        dtypes = [('f4', A.float32()), ('f8', A.float64())]
        num_values = 100

        for numpy_dtype, arrow_dtype in dtypes:
            values = np.random.randn(num_values)
            data[numpy_dtype] = values.astype(numpy_dtype)
            fields.append(A.Field.from_py(numpy_dtype, arrow_dtype))

        df = pd.DataFrame(data)
        schema = A.Schema.from_fields(fields)
        self._check_pandas_roundtrip(df, expected_schema=schema)

    def test_float_nulls(self):
        num_values = 100

        null_mask = np.random.randint(0, 10, size=num_values) < 3
        dtypes = [('f4', A.float32()), ('f8', A.float64())]
        names = ['f4', 'f8']
        expected_cols = []

        arrays = []
        fields = []
        for name, arrow_dtype in dtypes:
            values = np.random.randn(num_values).astype(name)

            arr = A.Array.from_numpy(values, null_mask)
            arrays.append(arr)
            fields.append(A.Field.from_py(name, arrow_dtype))
            values[null_mask] = np.nan

            expected_cols.append(values)

        ex_frame = pd.DataFrame(dict(zip(names, expected_cols)),
                                columns=names)

        table = A.Table.from_arrays(arrays, names)
        assert table.schema.equals(A.Schema.from_fields(fields))
        result = table.to_pandas()
        tm.assert_frame_equal(result, ex_frame)

    def test_integer_no_nulls(self):
        data = OrderedDict()
        fields = []

        numpy_dtypes = [
            ('i1', A.int8()), ('i2', A.int16()),
            ('i4', A.int32()), ('i8', A.int64()),
            ('u1', A.uint8()), ('u2', A.uint16()),
            ('u4', A.uint32()), ('u8', A.uint64()),
            ('longlong', A.int64()), ('ulonglong', A.uint64())
        ]
        num_values = 100

        for dtype, arrow_dtype in numpy_dtypes:
            info = np.iinfo(dtype)
            values = np.random.randint(info.min,
                                       min(info.max, np.iinfo('i8').max),
                                       size=num_values)
            data[dtype] = values.astype(dtype)
            fields.append(A.Field.from_py(dtype, arrow_dtype))

        df = pd.DataFrame(data)
        schema = A.Schema.from_fields(fields)
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

            arr = A.Array.from_numpy(values, null_mask)
            arrays.append(arr)

            expected = values.astype('f8')
            expected[null_mask] = np.nan

            expected_cols.append(expected)

        ex_frame = pd.DataFrame(dict(zip(int_dtypes, expected_cols)),
                                columns=int_dtypes)

        table = A.Table.from_arrays(arrays, int_dtypes)
        result = table.to_pandas()

        tm.assert_frame_equal(result, ex_frame)

    def test_boolean_no_nulls(self):
        num_values = 100

        np.random.seed(0)

        df = pd.DataFrame({'bools': np.random.randn(num_values) > 0})
        field = A.Field.from_py('bools', A.bool_())
        schema = A.Schema.from_fields([field])
        self._check_pandas_roundtrip(df, expected_schema=schema)

    def test_boolean_nulls(self):
        # pandas requires upcast to object dtype
        num_values = 100
        np.random.seed(0)

        mask = np.random.randint(0, 10, size=num_values) < 3
        values = np.random.randint(0, 10, size=num_values) < 5

        arr = A.Array.from_numpy(values, mask)

        expected = values.astype(object)
        expected[mask] = None

        field = A.Field.from_py('bools', A.bool_())
        schema = A.Schema.from_fields([field])
        ex_frame = pd.DataFrame({'bools': expected})

        table = A.Table.from_arrays([arr], ['bools'])
        assert table.schema.equals(schema)
        result = table.to_pandas()

        tm.assert_frame_equal(result, ex_frame)

    def test_boolean_object_nulls(self):
        arr = np.array([False, None, True] * 100, dtype=object)
        df = pd.DataFrame({'bools': arr})
        field = A.Field.from_py('bools', A.bool_())
        schema = A.Schema.from_fields([field])
        self._check_pandas_roundtrip(df, expected_schema=schema)

    def test_unicode(self):
        repeats = 1000
        values = [u'foo', None, u'bar', u'mañana', np.nan]
        df = pd.DataFrame({'strings': values * repeats})
        field = A.Field.from_py('strings', A.string())
        schema = A.Schema.from_fields([field])

        self._check_pandas_roundtrip(df, expected_schema=schema)

    def test_bytes_to_binary(self):
        values = [u('qux'), b'foo', None, 'bar', 'qux', np.nan]
        df = pd.DataFrame({'strings': values})

        table = A.Table.from_pandas(df)
        assert table[0].type == A.binary()

        values2 = [b'qux', b'foo', None, b'bar', b'qux', np.nan]
        expected = pd.DataFrame({'strings': values2})
        self._check_pandas_roundtrip(df, expected)

    def test_fixed_size_bytes(self):
        values = [b'foo', None, b'bar', None, None, b'hey']
        df = pd.DataFrame({'strings': values})
        schema = A.Schema.from_fields([A.field('strings', A.binary(3))])
        table = A.Table.from_pandas(df, schema=schema)
        assert table.schema[0].type == schema[0].type
        assert table.schema[0].name == schema[0].name
        result = table.to_pandas()
        tm.assert_frame_equal(result, df)

    def test_fixed_size_bytes_does_not_accept_varying_lengths(self):
        values = [b'foo', None, b'ba', None, None, b'hey']
        df = pd.DataFrame({'strings': values})
        schema = A.Schema.from_fields([A.field('strings', A.binary(3))])
        with self.assertRaises(A.ArrowInvalid):
            A.Table.from_pandas(df, schema=schema)

    def test_timestamps_notimezone_no_nulls(self):
        df = pd.DataFrame({
            'datetime64': np.array([
                '2007-07-13T01:23:34.123',
                '2006-01-13T12:34:56.432',
                '2010-08-13T05:46:57.437'],
                dtype='datetime64[ms]')
            })
        field = A.Field.from_py('datetime64', A.timestamp('ms'))
        schema = A.Schema.from_fields([field])
        self._check_pandas_roundtrip(df, timestamps_to_ms=True,
                                     expected_schema=schema)

        df = pd.DataFrame({
            'datetime64': np.array([
                '2007-07-13T01:23:34.123456789',
                '2006-01-13T12:34:56.432539784',
                '2010-08-13T05:46:57.437699912'],
                dtype='datetime64[ns]')
            })
        field = A.Field.from_py('datetime64', A.timestamp('ns'))
        schema = A.Schema.from_fields([field])
        self._check_pandas_roundtrip(df, timestamps_to_ms=False,
                                     expected_schema=schema)

    def test_timestamps_notimezone_nulls(self):
        df = pd.DataFrame({
            'datetime64': np.array([
                '2007-07-13T01:23:34.123',
                None,
                '2010-08-13T05:46:57.437'],
                dtype='datetime64[ms]')
            })
        field = A.Field.from_py('datetime64', A.timestamp('ms'))
        schema = A.Schema.from_fields([field])
        self._check_pandas_roundtrip(df, timestamps_to_ms=True,
                                     expected_schema=schema)

        df = pd.DataFrame({
            'datetime64': np.array([
                '2007-07-13T01:23:34.123456789',
                None,
                '2010-08-13T05:46:57.437699912'],
                dtype='datetime64[ns]')
            })
        field = A.Field.from_py('datetime64', A.timestamp('ns'))
        schema = A.Schema.from_fields([field])
        self._check_pandas_roundtrip(df, timestamps_to_ms=False,
                                     expected_schema=schema)

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
        self._check_pandas_roundtrip(df, timestamps_to_ms=True)

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
        self._check_pandas_roundtrip(df, timestamps_to_ms=False)

    def test_date(self):
        df = pd.DataFrame({
            'date': [datetime.date(2000, 1, 1),
                     None,
                     datetime.date(1970, 1, 1),
                     datetime.date(2040, 2, 26)]})
        table = A.Table.from_pandas(df)
        field = A.Field.from_py('date', A.date64())
        schema = A.Schema.from_fields([field])
        assert table.schema.equals(schema)
        result = table.to_pandas()
        expected = df.copy()
        expected['date'] = pd.to_datetime(df['date'])
        tm.assert_frame_equal(result, expected)

    def test_column_of_arrays(self):
        df, schema = dataframe_with_arrays()
        self._check_pandas_roundtrip(df, schema=schema, expected_schema=schema)
        table = A.Table.from_pandas(df, schema=schema)
        assert table.schema.equals(schema)

        for column in df.columns:
            field = schema.field_by_name(column)
            self._check_array_roundtrip(df[column], type=field.type)

    def test_column_of_lists(self):
        df, schema = dataframe_with_lists()
        self._check_pandas_roundtrip(df, schema=schema, expected_schema=schema)
        table = A.Table.from_pandas(df, schema=schema)
        assert table.schema.equals(schema)

        for column in df.columns:
            field = schema.field_by_name(column)
            self._check_array_roundtrip(df[column], type=field.type)

    def test_threaded_conversion(self):
        df = _alltypes_example()
        self._check_pandas_roundtrip(df, nthreads=2,
                                     timestamps_to_ms=False)

    def test_category(self):
        repeats = 5
        v1 = ['foo', None, 'bar', 'qux', np.nan]
        v2 = [4, 5, 6, 7, 8]
        v3 = [b'foo', None, b'bar', b'qux', np.nan]
        df = pd.DataFrame({'cat_strings': pd.Categorical(v1 * repeats),
                           'cat_ints': pd.Categorical(v2 * repeats),
                           'cat_binary': pd.Categorical(v3 * repeats),
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
        with self.assertRaises(A.ArrowException):
            A.Table.from_pandas(data)

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
