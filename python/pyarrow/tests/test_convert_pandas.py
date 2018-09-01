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
import decimal
import json
import multiprocessing as mp
from collections import OrderedDict
from datetime import date, datetime, time, timedelta

import numpy as np
import numpy.testing as npt
import pandas as pd
import pandas.util.testing as tm
import pytest

import pyarrow as pa
import pyarrow.types as patypes
from pyarrow.compat import PY2

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


def _check_pandas_roundtrip(df, expected=None, use_threads=True,
                            expected_schema=None,
                            check_dtype=True, schema=None,
                            preserve_index=False,
                            as_batch=False):
    klass = pa.RecordBatch if as_batch else pa.Table
    table = klass.from_pandas(df, schema=schema,
                              preserve_index=preserve_index,
                              nthreads=2 if use_threads else 1)
    result = table.to_pandas(use_threads=use_threads)

    if expected_schema:
        # all occurences of _check_pandas_roundtrip passes expected_schema
        # without the pandas generated key-value metadata, so we need to
        # add it before checking schema equality
        expected_schema = expected_schema.add_metadata(table.schema.metadata)
        assert table.schema.equals(expected_schema)

    if expected is None:
        expected = df

    tm.assert_frame_equal(result, expected, check_dtype=check_dtype,
                          check_index_type=('equiv' if preserve_index
                                            else False))


def _check_series_roundtrip(s, type_=None, expected_pa_type=None):
    arr = pa.array(s, from_pandas=True, type=type_)

    if type_ is not None and expected_pa_type is None:
        expected_pa_type = type_

    if expected_pa_type is not None:
        assert arr.type == expected_pa_type

    result = pd.Series(arr.to_pandas(), name=s.name)
    if patypes.is_timestamp(arr.type) and arr.type.tz is not None:
        result = (result.dt.tz_localize('utc')
                  .dt.tz_convert(arr.type.tz))

    tm.assert_series_equal(s, result)


def _check_array_roundtrip(values, expected=None, mask=None,
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


def _check_array_from_pandas_roundtrip(np_array, type=None):
    arr = pa.array(np_array, from_pandas=True, type=type)
    result = arr.to_pandas()
    npt.assert_array_equal(result, np_array)


class TestConvertMetadata(object):
    """
    Conversion tests for Pandas metadata & indices.
    """

    def test_non_string_columns(self):
        df = pd.DataFrame({0: [1, 2, 3]})
        table = pa.Table.from_pandas(df)
        assert table.column(0).name == '0'

    def test_from_pandas_with_columns(self):
        df = pd.DataFrame({0: [1, 2, 3], 1: [1, 3, 3], 2: [2, 4, 5]})

        table = pa.Table.from_pandas(df, columns=[0, 1])
        expected = pa.Table.from_pandas(df[[0, 1]])
        assert expected.equals(table)

        record_batch_table = pa.RecordBatch.from_pandas(df, columns=[0, 1])
        record_batch_expected = pa.RecordBatch.from_pandas(df[[0, 1]])
        assert record_batch_expected.equals(record_batch_table)

    def test_column_index_names_are_preserved(self):
        df = pd.DataFrame({'data': [1, 2, 3]})
        df.columns.names = ['a']
        _check_pandas_roundtrip(df, preserve_index=True)

    def test_multiindex_columns(self):
        columns = pd.MultiIndex.from_arrays([
            ['one', 'two'], ['X', 'Y']
        ])
        df = pd.DataFrame([(1, 'a'), (2, 'b'), (3, 'c')], columns=columns)
        _check_pandas_roundtrip(df, preserve_index=True)

    def test_multiindex_columns_with_dtypes(self):
        columns = pd.MultiIndex.from_arrays(
            [
                ['one', 'two'],
                pd.DatetimeIndex(['2017-08-01', '2017-08-02']),
            ],
            names=['level_1', 'level_2'],
        )
        df = pd.DataFrame([(1, 'a'), (2, 'b'), (3, 'c')], columns=columns)
        _check_pandas_roundtrip(df, preserve_index=True)

    def test_multiindex_columns_unicode(self):
        columns = pd.MultiIndex.from_arrays([[u'あ', u'い'], ['X', 'Y']])
        df = pd.DataFrame([(1, 'a'), (2, 'b'), (3, 'c')], columns=columns)
        _check_pandas_roundtrip(df, preserve_index=True)

    def test_integer_index_column(self):
        df = pd.DataFrame([(1, 'a'), (2, 'b'), (3, 'c')])
        _check_pandas_roundtrip(df, preserve_index=True)

    def test_index_metadata_field_name(self):
        # test None case, and strangely named non-index columns
        df = pd.DataFrame(
            [(1, 'a', 3.1), (2, 'b', 2.2), (3, 'c', 1.3)],
            index=pd.MultiIndex.from_arrays(
                [['c', 'b', 'a'], [3, 2, 1]],
                names=[None, 'foo']
            ),
            columns=['a', None, '__index_level_0__'],
        )
        t = pa.Table.from_pandas(df, preserve_index=True)
        raw_metadata = t.schema.metadata

        js = json.loads(raw_metadata[b'pandas'].decode('utf8'))

        col1, col2, col3, idx0, foo = js['columns']

        assert col1['name'] == 'a'
        assert col1['name'] == col1['field_name']

        assert col2['name'] is None
        assert col2['field_name'] == 'None'

        assert col3['name'] == '__index_level_0__'
        assert col3['name'] == col3['field_name']

        idx0_name, foo_name = js['index_columns']
        assert idx0_name == '__index_level_0__'
        assert idx0['field_name'] == idx0_name
        assert idx0['name'] is None

        assert foo_name == 'foo'
        assert foo['field_name'] == foo_name
        assert foo['name'] == foo_name

    def test_categorical_column_index(self):
        df = pd.DataFrame(
            [(1, 'a', 2.0), (2, 'b', 3.0), (3, 'c', 4.0)],
            columns=pd.Index(list('def'), dtype='category')
        )
        t = pa.Table.from_pandas(df, preserve_index=True)
        raw_metadata = t.schema.metadata
        js = json.loads(raw_metadata[b'pandas'].decode('utf8'))

        column_indexes, = js['column_indexes']
        assert column_indexes['name'] is None
        assert column_indexes['pandas_type'] == 'categorical'
        assert column_indexes['numpy_type'] == 'int8'

        md = column_indexes['metadata']
        assert md['num_categories'] == 3
        assert md['ordered'] is False

    def test_string_column_index(self):
        df = pd.DataFrame(
            [(1, 'a', 2.0), (2, 'b', 3.0), (3, 'c', 4.0)],
            columns=pd.Index(list('def'), name='stringz')
        )
        t = pa.Table.from_pandas(df, preserve_index=True)
        raw_metadata = t.schema.metadata
        js = json.loads(raw_metadata[b'pandas'].decode('utf8'))

        column_indexes, = js['column_indexes']
        assert column_indexes['name'] == 'stringz'
        assert column_indexes['name'] == column_indexes['field_name']
        assert column_indexes['pandas_type'] == ('bytes' if PY2 else 'unicode')
        assert column_indexes['numpy_type'] == 'object'

        md = column_indexes['metadata']

        if not PY2:
            assert len(md) == 1
            assert md['encoding'] == 'UTF-8'
        else:
            assert md is None or 'encoding' not in md

    def test_datetimetz_column_index(self):
        df = pd.DataFrame(
            [(1, 'a', 2.0), (2, 'b', 3.0), (3, 'c', 4.0)],
            columns=pd.date_range(
                start='2017-01-01', periods=3, tz='America/New_York'
            )
        )
        t = pa.Table.from_pandas(df, preserve_index=True)
        raw_metadata = t.schema.metadata
        js = json.loads(raw_metadata[b'pandas'].decode('utf8'))

        column_indexes, = js['column_indexes']
        assert column_indexes['name'] is None
        assert column_indexes['pandas_type'] == 'datetimetz'
        assert column_indexes['numpy_type'] == 'datetime64[ns]'

        md = column_indexes['metadata']
        assert md['timezone'] == 'America/New_York'

    def test_datetimetz_row_index(self):
        df = pd.DataFrame({
            'a': pd.date_range(
                start='2017-01-01', periods=3, tz='America/New_York'
            )
        })
        df = df.set_index('a')

        _check_pandas_roundtrip(df, preserve_index=True)

    def test_categorical_row_index(self):
        df = pd.DataFrame({'a': [1, 2, 3], 'b': [1, 2, 3]})
        df['a'] = df.a.astype('category')
        df = df.set_index('a')

        _check_pandas_roundtrip(df, preserve_index=True)

    def test_duplicate_column_names_does_not_crash(self):
        df = pd.DataFrame([(1, 'a'), (2, 'b')], columns=list('aa'))
        with pytest.raises(ValueError):
            pa.Table.from_pandas(df)

    def test_dictionary_indices_boundscheck(self):
        # ARROW-1658. No validation of indices leads to segfaults in pandas
        indices = [[0, 1], [0, -1]]

        for inds in indices:
            arr = pa.DictionaryArray.from_arrays(inds, ['a'], safe=False)
            batch = pa.RecordBatch.from_arrays([arr], ['foo'])
            table = pa.Table.from_batches([batch, batch, batch])

            with pytest.raises(pa.ArrowInvalid):
                arr.to_pandas()

            with pytest.raises(pa.ArrowInvalid):
                table.to_pandas()

    def test_unicode_with_unicode_column_and_index(self):
        df = pd.DataFrame({u'あ': [u'い']}, index=[u'う'])

        _check_pandas_roundtrip(df, preserve_index=True)

    def test_mixed_unicode_column_names(self):
        df = pd.DataFrame({u'あ': [u'い'], b'a': 1}, index=[u'う'])

        # TODO(phillipc): Should this raise?
        with pytest.raises(AssertionError):
            _check_pandas_roundtrip(df, preserve_index=True)

    def test_binary_column_name(self):
        column_data = [u'い']
        key = u'あ'.encode('utf8')
        data = {key: column_data}
        df = pd.DataFrame(data)

        # we can't use _check_pandas_roundtrip here because our metdata
        # is always decoded as utf8: even if binary goes in, utf8 comes out
        t = pa.Table.from_pandas(df, preserve_index=True)
        df2 = t.to_pandas()
        assert df.values[0] == df2.values[0]
        assert df.index.values[0] == df2.index.values[0]
        assert df.columns[0] == key

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

    def test_table_column_subset_metadata(self):
        # ARROW-1883
        df = pd.DataFrame({
            'a': [1, 2, 3],
            'b': pd.date_range("2017-01-01", periods=3, tz='Europe/Brussels')})
        table = pa.Table.from_pandas(df)

        table_subset = table.remove_column(1)
        result = table_subset.to_pandas()
        tm.assert_frame_equal(result, df[['a']])

        table_subset2 = table_subset.remove_column(1)
        result = table_subset2.to_pandas()
        tm.assert_frame_equal(result, df[['a']])

        # non-default index
        for index in [
                pd.Index(['a', 'b', 'c'], name='index'),
                pd.date_range("2017-01-01", periods=3, tz='Europe/Brussels')]:
            df = pd.DataFrame({'a': [1, 2, 3],
                               'b': [.1, .2, .3]}, index=index)
            table = pa.Table.from_pandas(df)

            table_subset = table.remove_column(1)
            result = table_subset.to_pandas()
            tm.assert_frame_equal(result, df[['a']])

            table_subset2 = table_subset.remove_column(1)
            result = table_subset2.to_pandas()
            tm.assert_frame_equal(result, df[['a']].reset_index(drop=True))

    def test_empty_list_metadata(self):
        # Create table with array of empty lists, forced to have type
        # list(string) in pyarrow
        c1 = [["test"], ["a", "b"], None]
        c2 = [[], [], []]
        arrays = OrderedDict([
            ('c1', pa.array(c1, type=pa.list_(pa.string()))),
            ('c2', pa.array(c2, type=pa.list_(pa.string()))),
        ])
        rb = pa.RecordBatch.from_arrays(
            list(arrays.values()),
            list(arrays.keys())
        )
        tbl = pa.Table.from_batches([rb])

        # First roundtrip changes schema, because pandas cannot preserve the
        # type of empty lists
        df = tbl.to_pandas()
        tbl2 = pa.Table.from_pandas(df, preserve_index=True)
        md2 = json.loads(tbl2.schema.metadata[b'pandas'].decode('utf8'))

        # Second roundtrip
        df2 = tbl2.to_pandas()
        expected = pd.DataFrame(OrderedDict([('c1', c1), ('c2', c2)]))

        tm.assert_frame_equal(df2, expected)

        assert md2['columns'] == [
            {
                'name': 'c1',
                'field_name': 'c1',
                'metadata': None,
                'numpy_type': 'object',
                'pandas_type': 'list[unicode]',
            },
            {
                'name': 'c2',
                'field_name': 'c2',
                'metadata': None,
                'numpy_type': 'object',
                'pandas_type': 'list[empty]',
            },
            {
                'name': None,
                'field_name': '__index_level_0__',
                'metadata': None,
                'numpy_type': 'int64',
                'pandas_type': 'int64',
            }
        ]


class TestConvertPrimitiveTypes(object):
    """
    Conversion tests for primitive (e.g. numeric) types.
    """

    def test_float_no_nulls(self):
        data = {}
        fields = []
        dtypes = [('f2', pa.float16()),
                  ('f4', pa.float32()),
                  ('f8', pa.float64())]
        num_values = 100

        for numpy_dtype, arrow_dtype in dtypes:
            values = np.random.randn(num_values)
            data[numpy_dtype] = values.astype(numpy_dtype)
            fields.append(pa.field(numpy_dtype, arrow_dtype))

        df = pd.DataFrame(data)
        schema = pa.schema(fields)
        _check_pandas_roundtrip(df, expected_schema=schema)

    def test_float_nulls(self):
        num_values = 100

        null_mask = np.random.randint(0, 10, size=num_values) < 3
        dtypes = [('f2', pa.float16()),
                  ('f4', pa.float32()),
                  ('f8', pa.float64())]
        names = ['f2', 'f4', 'f8']
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

    def test_float_nulls_to_ints(self):
        # ARROW-2135
        df = pd.DataFrame({"a": [1.0, 2.0, pd.np.NaN]})
        schema = pa.schema([pa.field("a", pa.int16(), nullable=True)])
        table = pa.Table.from_pandas(df, schema=schema)
        assert table[0].to_pylist() == [1, 2, None]
        tm.assert_frame_equal(df, table.to_pandas())

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
        _check_pandas_roundtrip(df, expected_schema=schema)

    def test_all_integer_types(self):
        # Test all Numpy integer aliases
        data = OrderedDict()
        numpy_dtypes = ['i1', 'i2', 'i4', 'i8', 'u1', 'u2', 'u4', 'u8',
                        'byte', 'ubyte', 'short', 'ushort', 'intc', 'uintc',
                        'int_', 'uint', 'longlong', 'ulonglong']
        for dtype in numpy_dtypes:
            data[dtype] = np.arange(12, dtype=dtype)
        df = pd.DataFrame(data)
        _check_pandas_roundtrip(df)

        # Do the same with pa.array()
        # (for some reason, it doesn't use the same code paths at all)
        for np_arr in data.values():
            arr = pa.array(np_arr)
            assert arr.to_pylist() == np_arr.tolist()

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
        _check_pandas_roundtrip(df, expected_schema=schema)

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

    def test_float_object_nulls(self):
        arr = np.array([None, 1.5, np.float64(3.5)] * 5, dtype=object)
        df = pd.DataFrame({'floats': arr})
        expected = pd.DataFrame({'floats': pd.to_numeric(arr)})
        field = pa.field('floats', pa.float64())
        schema = pa.schema([field])
        _check_pandas_roundtrip(df, expected=expected,
                                expected_schema=schema)

    def test_int_object_nulls(self):
        arr = np.array([None, 1, np.int64(3)] * 5, dtype=object)
        df = pd.DataFrame({'ints': arr})
        expected = pd.DataFrame({'ints': pd.to_numeric(arr)})
        field = pa.field('ints', pa.int64())
        schema = pa.schema([field])
        _check_pandas_roundtrip(df, expected=expected,
                                expected_schema=schema)

    def test_boolean_object_nulls(self):
        arr = np.array([False, None, True] * 100, dtype=object)
        df = pd.DataFrame({'bools': arr})
        field = pa.field('bools', pa.bool_())
        schema = pa.schema([field])
        _check_pandas_roundtrip(df, expected_schema=schema)

    def test_all_nulls_cast_numeric(self):
        arr = np.array([None], dtype=object)

        def _check_type(t):
            a2 = pa.array(arr, type=t)
            assert a2.type == t
            assert a2[0].as_py() is None

        _check_type(pa.int32())
        _check_type(pa.float64())

    def test_half_floats_from_numpy(self):
        arr = np.array([1.5, np.nan], dtype=np.float16)
        a = pa.array(arr, type=pa.float16())
        x, y = a.to_pylist()
        assert isinstance(x, np.float16)
        assert x == 1.5
        assert isinstance(y, np.float16)
        assert np.isnan(y)

        a = pa.array(arr, type=pa.float16(), from_pandas=True)
        x, y = a.to_pylist()
        assert isinstance(x, np.float16)
        assert x == 1.5
        assert y is None


@pytest.mark.parametrize('dtype',
                         ['i1', 'i2', 'i4', 'i8', 'u1', 'u2', 'u4', 'u8'])
def test_array_integer_object_nulls_option(dtype):
    num_values = 100

    null_mask = np.random.randint(0, 10, size=num_values) < 3
    values = np.random.randint(0, 100, size=num_values, dtype=dtype)

    array = pa.array(values, mask=null_mask)

    if null_mask.any():
        expected = values.astype('O')
        expected[null_mask] = None
    else:
        expected = values

    result = array.to_pandas(integer_object_nulls=True)

    np.testing.assert_equal(result, expected)


@pytest.mark.parametrize('dtype',
                         ['i1', 'i2', 'i4', 'i8', 'u1', 'u2', 'u4', 'u8'])
def test_table_integer_object_nulls_option(dtype):
    num_values = 100

    null_mask = np.random.randint(0, 10, size=num_values) < 3
    values = np.random.randint(0, 100, size=num_values, dtype=dtype)

    array = pa.array(values, mask=null_mask)

    if null_mask.any():
        expected = values.astype('O')
        expected[null_mask] = None
    else:
        expected = values

    expected = pd.DataFrame({dtype: expected})

    table = pa.Table.from_arrays([array], [dtype])
    result = table.to_pandas(integer_object_nulls=True)

    tm.assert_frame_equal(result, expected)


class TestConvertDateTimeLikeTypes(object):
    """
    Conversion tests for datetime- and timestamp-like types (date64, etc.).
    """

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
        _check_pandas_roundtrip(
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
        _check_pandas_roundtrip(
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
        _check_pandas_roundtrip(df)

        _check_series_roundtrip(df['datetime64'])

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

        _check_pandas_roundtrip(df)

    def test_python_datetime(self):
        # ARROW-2106
        date_array = [datetime.today() + timedelta(days=x) for x in range(10)]
        df = pd.DataFrame({
            'datetime': pd.Series(date_array, dtype=object)
        })

        table = pa.Table.from_pandas(df)
        assert isinstance(table[0].data.chunk(0), pa.TimestampArray)

        result = table.to_pandas()
        expected_df = pd.DataFrame({
            'datetime': date_array
        })
        tm.assert_frame_equal(expected_df, result)

    def test_python_datetime_subclass(self):

        class MyDatetime(datetime):
            # see https://github.com/pandas-dev/pandas/issues/21142
            nanosecond = 0.0

        date_array = [MyDatetime(2000, 1, 1, 1, 1, 1)]
        df = pd.DataFrame({"datetime": pd.Series(date_array, dtype=object)})

        table = pa.Table.from_pandas(df)
        assert isinstance(table[0].data.chunk(0), pa.TimestampArray)

        result = table.to_pandas()
        expected_df = pd.DataFrame({"datetime": date_array})

        # https://github.com/pandas-dev/pandas/issues/21142
        expected_df["datetime"] = pd.to_datetime(expected_df["datetime"])

        tm.assert_frame_equal(expected_df, result)

    def test_python_date_subclass(self):

        class MyDate(date):
            pass

        date_array = [MyDate(2000, 1, 1)]
        df = pd.DataFrame({"date": pd.Series(date_array, dtype=object)})

        table = pa.Table.from_pandas(df)
        assert isinstance(table[0].data.chunk(0), pa.Date32Array)

        result = table.to_pandas()
        expected_df = pd.DataFrame(
            {"date": np.array(["2000-01-01"], dtype="datetime64[ns]")}
        )
        tm.assert_frame_equal(expected_df, result)

    def test_datetime64_to_date32(self):
        # ARROW-1718
        arr = pa.array([date(2017, 10, 23), None])
        c = pa.Column.from_array("d", arr)
        s = c.to_pandas()

        arr2 = pa.Array.from_pandas(s, type=pa.date32())

        assert arr2.equals(arr.cast('date32'))

    @pytest.mark.parametrize('mask', [
        None,
        np.array([True, False, False]),
    ])
    def test_pandas_datetime_to_date64(self, mask):
        s = pd.to_datetime([
            '2018-05-10T00:00:00',
            '2018-05-11T00:00:00',
            '2018-05-12T00:00:00',
        ])
        arr = pa.Array.from_pandas(s, type=pa.date64(), mask=mask)

        data = np.array([
            date(2018, 5, 10),
            date(2018, 5, 11),
            date(2018, 5, 12)
        ])
        expected = pa.array(data, mask=mask, type=pa.date64())

        assert arr.equals(expected)

    @pytest.mark.parametrize('mask', [
        None,
        np.array([True, False, False])
    ])
    def test_pandas_datetime_to_date64_failures(self, mask):
        s = pd.to_datetime([
            '2018-05-10T10:24:01',
            '2018-05-11T10:24:01',
            '2018-05-12T10:24:01',
        ])

        expected_msg = 'Timestamp value had non-zero intraday milliseconds'
        with pytest.raises(pa.ArrowInvalid, match=expected_msg):
            pa.Array.from_pandas(s, type=pa.date64(), mask=mask)

    def test_date_infer(self):
        df = pd.DataFrame({
            'date': [date(2000, 1, 1),
                     None,
                     date(1970, 1, 1),
                     date(2040, 2, 26)]})
        table = pa.Table.from_pandas(df, preserve_index=False)
        field = pa.field('date', pa.date32())

        # schema's metadata is generated by from_pandas conversion
        expected_schema = pa.schema([field], metadata=table.schema.metadata)
        assert table.schema.equals(expected_schema)

        result = table.to_pandas()
        expected = df.copy()
        expected['date'] = pd.to_datetime(df['date'])
        tm.assert_frame_equal(result, expected)

    def test_date_mask(self):
        arr = np.array([date(2017, 4, 3), date(2017, 4, 4)],
                       dtype='datetime64[D]')
        mask = [True, False]
        result = pa.array(arr, mask=np.array(mask))
        expected = np.array([None, date(2017, 4, 4)], dtype='datetime64[D]')
        expected = pa.array(expected, from_pandas=True)
        assert expected.equals(result)

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

    def test_numpy_datetime64_columns(self):
        datetime64_ns = np.array([
                '2007-07-13T01:23:34.123456789',
                None,
                '2006-01-13T12:34:56.432539784',
                '2010-08-13T05:46:57.437699912'],
                dtype='datetime64[ns]')
        _check_array_from_pandas_roundtrip(datetime64_ns)

        datetime64_us = np.array([
                '2007-07-13T01:23:34.123456',
                None,
                '2006-01-13T12:34:56.432539',
                '2010-08-13T05:46:57.437699'],
                dtype='datetime64[us]')
        _check_array_from_pandas_roundtrip(datetime64_us)

        datetime64_ms = np.array([
                '2007-07-13T01:23:34.123',
                None,
                '2006-01-13T12:34:56.432',
                '2010-08-13T05:46:57.437'],
                dtype='datetime64[ms]')
        _check_array_from_pandas_roundtrip(datetime64_ms)

        datetime64_s = np.array([
                '2007-07-13T01:23:34',
                None,
                '2006-01-13T12:34:56',
                '2010-08-13T05:46:57'],
                dtype='datetime64[s]')
        _check_array_from_pandas_roundtrip(datetime64_s)

    @pytest.mark.parametrize('dtype', [pa.date32(), pa.date64()])
    def test_numpy_datetime64_day_unit(self, dtype):
        datetime64_d = np.array([
                '2007-07-13',
                None,
                '2006-01-15',
                '2010-08-19'],
                dtype='datetime64[D]')
        _check_array_from_pandas_roundtrip(datetime64_d, type=dtype)

    def test_array_from_pandas_date_with_mask(self):
        m = np.array([True, False, True])
        data = pd.Series([
            date(1990, 1, 1),
            date(1991, 1, 1),
            date(1992, 1, 1)
        ])

        result = pa.Array.from_pandas(data, mask=m)

        expected = pd.Series([None, date(1991, 1, 1), None])
        assert pa.Array.from_pandas(expected).equals(result)

    def test_fixed_offset_timezone(self):
        df = pd.DataFrame({
            'a': [
                pd.Timestamp('2012-11-11 00:00:00+01:00'),
                pd.NaT
                ]
             })
        _check_pandas_roundtrip(df)
        _check_serialize_components_roundtrip(df)

# ----------------------------------------------------------------------
# Conversion tests for string and binary types.


class TestConvertStringLikeTypes(object):

    def test_pandas_unicode(self):
        repeats = 1000
        values = [u'foo', None, u'bar', u'mañana', np.nan]
        df = pd.DataFrame({'strings': values * repeats})
        field = pa.field('strings', pa.string())
        schema = pa.schema([field])

        _check_pandas_roundtrip(df, expected_schema=schema)

    def test_bytes_to_binary(self):
        values = [u'qux', b'foo', None, bytearray(b'barz'), 'qux', np.nan]
        df = pd.DataFrame({'strings': values})

        table = pa.Table.from_pandas(df)
        assert table[0].type == pa.binary()

        values2 = [b'qux', b'foo', None, b'barz', b'qux', np.nan]
        expected = pd.DataFrame({'strings': values2})
        _check_pandas_roundtrip(df, expected)

    @pytest.mark.large_memory
    def test_bytes_exceed_2gb(self):
        v1 = b'x' * 100000000
        v2 = b'x' * 147483646

        # ARROW-2227, hit exactly 2GB on the nose
        df = pd.DataFrame({
            'strings': [v1] * 20 + [v2] + ['x'] * 20
        })
        arr = pa.array(df['strings'])
        assert isinstance(arr, pa.ChunkedArray)
        assert arr.num_chunks == 2
        arr = None

        table = pa.Table.from_pandas(df)
        assert table[0].data.num_chunks == 2

    def test_fixed_size_bytes(self):
        values = [b'foo', None, bytearray(b'bar'), None, None, b'hey']
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
        with pytest.raises(pa.ArrowInvalid):
            pa.Table.from_pandas(df, schema=schema)

    def test_variable_size_bytes(self):
        s = pd.Series([b'123', b'', b'a', None])
        _check_series_roundtrip(s, type_=pa.binary())

    def test_binary_from_bytearray(self):
        s = pd.Series([bytearray(b'123'), bytearray(b''), bytearray(b'a'),
                       None])
        # Explicitly set type
        _check_series_roundtrip(s, type_=pa.binary())
        # Infer type from bytearrays
        _check_series_roundtrip(s, expected_pa_type=pa.binary())

    def test_table_empty_str(self):
        values = ['', '', '', '', '']
        df = pd.DataFrame({'strings': values})
        field = pa.field('strings', pa.string())
        schema = pa.schema([field])
        table = pa.Table.from_pandas(df, schema=schema)

        result1 = table.to_pandas(strings_to_categorical=False)
        expected1 = pd.DataFrame({'strings': values})
        tm.assert_frame_equal(result1, expected1, check_dtype=True)

        result2 = table.to_pandas(strings_to_categorical=True)
        expected2 = pd.DataFrame({'strings': pd.Categorical(values)})
        tm.assert_frame_equal(result2, expected2, check_dtype=True)

    def test_selective_categoricals(self):
        values = ['', '', '', '', '']
        df = pd.DataFrame({'strings': values})
        field = pa.field('strings', pa.string())
        schema = pa.schema([field])
        table = pa.Table.from_pandas(df, schema=schema)
        expected_str = pd.DataFrame({'strings': values})
        expected_cat = pd.DataFrame({'strings': pd.Categorical(values)})

        result1 = table.to_pandas(categories=['strings'])
        tm.assert_frame_equal(result1, expected_cat, check_dtype=True)
        result2 = table.to_pandas(categories=[])
        tm.assert_frame_equal(result2, expected_str, check_dtype=True)
        result3 = table.to_pandas(categories=('strings',))
        tm.assert_frame_equal(result3, expected_cat, check_dtype=True)
        result4 = table.to_pandas(categories=tuple())
        tm.assert_frame_equal(result4, expected_str, check_dtype=True)

    def test_table_str_to_categorical_without_na(self):
        values = ['a', 'a', 'b', 'b', 'c']
        df = pd.DataFrame({'strings': values})
        field = pa.field('strings', pa.string())
        schema = pa.schema([field])
        table = pa.Table.from_pandas(df, schema=schema)

        result = table.to_pandas(strings_to_categorical=True)
        expected = pd.DataFrame({'strings': pd.Categorical(values)})
        tm.assert_frame_equal(result, expected, check_dtype=True)

        with pytest.raises(pa.ArrowInvalid):
            table.to_pandas(strings_to_categorical=True,
                            zero_copy_only=True)

    def test_table_str_to_categorical_with_na(self):
        values = [None, 'a', 'b', np.nan]
        df = pd.DataFrame({'strings': values})
        field = pa.field('strings', pa.string())
        schema = pa.schema([field])
        table = pa.Table.from_pandas(df, schema=schema)

        result = table.to_pandas(strings_to_categorical=True)
        expected = pd.DataFrame({'strings': pd.Categorical(values)})
        tm.assert_frame_equal(result, expected, check_dtype=True)

        with pytest.raises(pa.ArrowInvalid):
            table.to_pandas(strings_to_categorical=True,
                            zero_copy_only=True)

    # Regression test for ARROW-2101
    def test_array_of_bytes_to_strings(self):
        converted = pa.array(np.array([b'x'], dtype=object), pa.string())
        assert converted.type == pa.string()

    # Make sure that if an ndarray of bytes is passed to the array
    # constructor and the type is string, it will fail if those bytes
    # cannot be converted to utf-8
    def test_array_of_bytes_to_strings_bad_data(self):
        with pytest.raises(
                pa.lib.ArrowInvalid,
                match="was not a utf8 string"):
            pa.array(np.array([b'\x80\x81'], dtype=object), pa.string())

    def test_numpy_string_array_to_fixed_size_binary(self):
        arr = np.array([b'foo', b'bar', b'baz'], dtype='|S3')

        converted = pa.array(arr, type=pa.binary(3))
        expected = pa.array(list(arr), type=pa.binary(3))
        assert converted.equals(expected)

        mask = np.array([True, False, True])
        converted = pa.array(arr, type=pa.binary(3), mask=mask)
        expected = pa.array([b'foo', None, b'baz'], type=pa.binary(3))
        assert converted.equals(expected)

        with pytest.raises(pa.lib.ArrowInvalid,
                           match='Got bytestring of length 3 \(expected 4\)'):
            arr = np.array([b'foo', b'bar', b'baz'], dtype='|S3')
            pa.array(arr, type=pa.binary(4))

        with pytest.raises(pa.lib.ArrowInvalid,
                           match='Got bytestring of length 12 \(expected 3\)'):
            arr = np.array([b'foo', b'bar', b'baz'], dtype='|U3')
            pa.array(arr, type=pa.binary(3))


class TestConvertDecimalTypes(object):
    """
    Conversion test for decimal types.
    """
    decimal32 = [
        decimal.Decimal('-1234.123'),
        decimal.Decimal('1234.439')
    ]
    decimal64 = [
        decimal.Decimal('-129934.123331'),
        decimal.Decimal('129534.123731')
    ]
    decimal128 = [
        decimal.Decimal('394092382910493.12341234678'),
        decimal.Decimal('-314292388910493.12343437128')
    ]

    @pytest.mark.parametrize(('values', 'expected_type'), [
        pytest.param(decimal32, pa.decimal128(7, 3), id='decimal32'),
        pytest.param(decimal64, pa.decimal128(12, 6), id='decimal64'),
        pytest.param(decimal128, pa.decimal128(26, 11), id='decimal128')
    ])
    def test_decimal_from_pandas(self, values, expected_type):
        expected = pd.DataFrame({'decimals': values})
        table = pa.Table.from_pandas(expected, preserve_index=False)
        field = pa.field('decimals', expected_type)

        # schema's metadata is generated by from_pandas conversion
        expected_schema = pa.schema([field], metadata=table.schema.metadata)
        assert table.schema.equals(expected_schema)

    @pytest.mark.parametrize('values', [
        pytest.param(decimal32, id='decimal32'),
        pytest.param(decimal64, id='decimal64'),
        pytest.param(decimal128, id='decimal128')
    ])
    def test_decimal_to_pandas(self, values):
        expected = pd.DataFrame({'decimals': values})
        converted = pa.Table.from_pandas(expected)
        df = converted.to_pandas()
        tm.assert_frame_equal(df, expected)

    def test_decimal_fails_with_truncation(self):
        data1 = [decimal.Decimal('1.234')]
        type1 = pa.decimal128(10, 2)
        with pytest.raises(pa.ArrowInvalid):
            pa.array(data1, type=type1)

        data2 = [decimal.Decimal('1.2345')]
        type2 = pa.decimal128(10, 3)
        with pytest.raises(pa.ArrowInvalid):
            pa.array(data2, type=type2)

    def test_decimal_with_different_precisions(self):
        data = [
            decimal.Decimal('0.01'),
            decimal.Decimal('0.001'),
        ]
        series = pd.Series(data)
        array = pa.array(series)
        assert array.to_pylist() == data
        assert array.type == pa.decimal128(3, 3)

        array = pa.array(data, type=pa.decimal128(12, 5))
        expected = [decimal.Decimal('0.01000'), decimal.Decimal('0.00100')]
        assert array.to_pylist() == expected

    def test_decimal_with_None_explicit_type(self):
        series = pd.Series([decimal.Decimal('3.14'), None])
        _check_series_roundtrip(series, type_=pa.decimal128(12, 5))

        # Test that having all None values still produces decimal array
        series = pd.Series([None] * 2)
        _check_series_roundtrip(series, type_=pa.decimal128(12, 5))

    def test_decimal_with_None_infer_type(self):
        series = pd.Series([decimal.Decimal('3.14'), None])
        _check_series_roundtrip(series, expected_pa_type=pa.decimal128(3, 2))

    def test_strided_objects(self, tmpdir):
        # see ARROW-3053
        data = {
            'a': {0: 'a'},
            'b': {0: decimal.Decimal('0.0')}
        }

        # This yields strided objects
        df = pd.DataFrame.from_dict(data)
        _check_pandas_roundtrip(df)


class TestListTypes(object):
    """
    Conversion tests for list<> types.
    """

    def test_column_of_arrays(self):
        df, schema = dataframe_with_arrays()
        _check_pandas_roundtrip(df, schema=schema, expected_schema=schema)
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

        # schema's metadata is generated by from_pandas conversion
        expected_schema = schema.add_metadata(table.schema.metadata)
        assert table.schema.equals(expected_schema)

        for column in df.columns:
            field = schema.field_by_name(column)
            _check_array_roundtrip(df[column], type=field.type)

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
        _check_pandas_roundtrip(df, schema=schema, expected_schema=schema)
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

        # schema's metadata is generated by from_pandas conversion
        expected_schema = schema.add_metadata(table.schema.metadata)
        assert table.schema.equals(expected_schema)

        for column in df.columns:
            field = schema.field_by_name(column)
            _check_array_roundtrip(df[column], type=field.type)

    def test_column_of_lists_first_empty(self):
        # ARROW-2124
        num_lists = [[], [2, 3, 4], [3, 6, 7, 8], [], [2]]
        series = pd.Series([np.array(s, dtype=float) for s in num_lists])
        arr = pa.array(series)
        result = pd.Series(arr.to_pandas())
        tm.assert_series_equal(result, series)

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

        _check_array_roundtrip(arr)

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

    def test_nested_lists_all_empty(self):
        # ARROW-2128
        data = pd.Series([[], [], []])
        arr = pa.array(data)
        expected = pa.array(list(data))
        assert arr.equals(expected)
        assert arr.type == pa.list_(pa.null())

    def test_nested_list_first_empty(self):
        # ARROW-2711
        data = pd.Series([[], [u"a"]])
        arr = pa.array(data)
        expected = pa.array(list(data))
        assert arr.equals(expected)
        assert arr.type == pa.list_(pa.string())

    def test_nested_smaller_ints(self):
        # ARROW-1345, ARROW-2008, there were some type inference bugs happening
        # before
        data = pd.Series([np.array([1, 2, 3], dtype='i1'), None])
        result = pa.array(data)
        result2 = pa.array(data.values)
        expected = pa.array([[1, 2, 3], None], type=pa.list_(pa.int8()))
        assert result.equals(expected)
        assert result2.equals(expected)

        data3 = pd.Series([np.array([1, 2, 3], dtype='f4'), None])
        result3 = pa.array(data3)
        expected3 = pa.array([[1, 2, 3], None], type=pa.list_(pa.float32()))
        assert result3.equals(expected3)

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

        _check_pandas_roundtrip(df, expected_schema=expected_schema)

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

        _check_pandas_roundtrip(df, expected_schema=expected_schema)

    @pytest.mark.parametrize('t,data,expected', [
        (
            pa.int64,
            [[1, 2], [3], None],
            [None, [3], None]
        ),
        (
            pa.string,
            [[u'aaa', u'bb'], [u'c'], None],
            [None, [u'c'], None]
        ),
        (
            pa.null,
            [[None, None], [None], None],
            [None, [None], None]
        )
    ])
    def test_array_from_pandas_typed_array_with_mask(self, t, data, expected):
        m = np.array([True, False, True])

        s = pd.Series(data)
        result = pa.Array.from_pandas(s, mask=m, type=pa.list_(t()))

        assert pa.Array.from_pandas(expected,
                                    type=pa.list_(t())).equals(result)

    def test_empty_list_roundtrip(self):
        empty_list_array = np.empty((3,), dtype=object)
        empty_list_array.fill([])

        df = pd.DataFrame({'a': np.array(['1', '2', '3']),
                           'b': empty_list_array})
        tbl = pa.Table.from_pandas(df)

        result = tbl.to_pandas()

        tm.assert_frame_equal(result, df)

    def test_array_from_nested_arrays(self):
        df, schema = dataframe_with_arrays()
        for field in schema:
            arr = df[field.name].values
            expected = pa.array(list(arr), type=field.type)
            result = pa.array(arr)
            assert result.type == field.type  # == list<scalar>
            assert result.equals(expected)


class TestConvertStructTypes(object):
    """
    Conversion tests for struct types.
    """

    def test_to_pandas(self):
        ints = pa.array([None, 2, 3], type=pa.int64())
        strs = pa.array([u'a', None, u'c'], type=pa.string())
        bools = pa.array([True, False, None], type=pa.bool_())
        arr = pa.StructArray.from_arrays(
            [ints, strs, bools],
            ['ints', 'strs', 'bools'])

        expected = pd.Series([
            {'ints': None, 'strs': u'a', 'bools': True},
            {'ints': 2, 'strs': None, 'bools': False},
            {'ints': 3, 'strs': u'c', 'bools': None},
        ])

        series = pd.Series(arr.to_pandas())
        tm.assert_series_equal(series, expected)

    def test_from_numpy(self):
        dt = np.dtype([('x', np.int32),
                       (('y_title', 'y'), np.bool_)])
        ty = pa.struct([pa.field('x', pa.int32()),
                        pa.field('y', pa.bool_())])

        data = np.array([], dtype=dt)
        arr = pa.array(data, type=ty)
        assert arr.to_pylist() == []

        data = np.array([(42, True), (43, False)], dtype=dt)
        arr = pa.array(data, type=ty)
        assert arr.to_pylist() == [{'x': 42, 'y': True},
                                   {'x': 43, 'y': False}]

        # With mask
        arr = pa.array(data, mask=np.bool_([False, True]), type=ty)
        assert arr.to_pylist() == [{'x': 42, 'y': True}, None]

        # Trivial struct type
        dt = np.dtype([])
        ty = pa.struct([])

        data = np.array([], dtype=dt)
        arr = pa.array(data, type=ty)
        assert arr.to_pylist() == []

        data = np.array([(), ()], dtype=dt)
        arr = pa.array(data, type=ty)
        assert arr.to_pylist() == [{}, {}]

    def test_from_numpy_nested(self):
        dt = np.dtype([('x', np.dtype([('xx', np.int8),
                                       ('yy', np.bool_)])),
                       ('y', np.int16)])
        ty = pa.struct([pa.field('x', pa.struct([pa.field('xx', pa.int8()),
                                                 pa.field('yy', pa.bool_())])),
                        pa.field('y', pa.int16())])

        data = np.array([], dtype=dt)
        arr = pa.array(data, type=ty)
        assert arr.to_pylist() == []

        data = np.array([((1, True), 2), ((3, False), 4)], dtype=dt)
        arr = pa.array(data, type=ty)
        assert arr.to_pylist() == [{'x': {'xx': 1, 'yy': True}, 'y': 2},
                                   {'x': {'xx': 3, 'yy': False}, 'y': 4}]

    @pytest.mark.large_memory
    def test_from_numpy_large(self):
        # Exercise rechunking + nulls
        target_size = 3 * 1024**3  # 4GB
        dt = np.dtype([('x', np.float64), ('y', 'object')])
        bs = 65536 - dt.itemsize
        block = b'.' * bs
        n = target_size // (bs + dt.itemsize)
        data = np.zeros(n, dtype=dt)
        data['x'] = np.random.random_sample(n)
        data['y'] = block
        # Add implicit nulls
        data['x'][data['x'] < 0.2] = np.nan

        ty = pa.struct([pa.field('x', pa.float64()),
                        pa.field('y', pa.binary(bs))])
        arr = pa.array(data, type=ty, from_pandas=True)
        assert arr.num_chunks == 2

        def iter_chunked_array(arr):
            for chunk in arr.iterchunks():
                for item in chunk:
                    yield item

        def check(arr, data, mask=None):
            assert len(arr) == len(data)
            xs = data['x']
            ys = data['y']
            for i, obj in enumerate(iter_chunked_array(arr)):
                try:
                    d = obj.as_py()
                    if mask is not None and mask[i]:
                        assert d is None
                    else:
                        x = xs[i]
                        if np.isnan(x):
                            assert d['x'] is None
                        else:
                            assert d['x'] == x
                        assert d['y'] == ys[i]
                except Exception:
                    print("Failed at index", i)
                    raise

        check(arr, data)
        del arr

        # Now with explicit mask
        mask = np.random.random_sample(n) < 0.2
        arr = pa.array(data, type=ty, mask=mask, from_pandas=True)
        assert arr.num_chunks == 2

        check(arr, data, mask)
        del arr

    def test_from_numpy_bad_input(self):
        ty = pa.struct([pa.field('x', pa.int32()),
                        pa.field('y', pa.bool_())])
        dt = np.dtype([('x', np.int32),
                       ('z', np.bool_)])

        data = np.array([], dtype=dt)
        with pytest.raises(TypeError,
                           match="Missing field 'y'"):
            pa.array(data, type=ty)
        data = np.int32([])
        with pytest.raises(TypeError,
                           match="Expected struct array"):
            pa.array(data, type=ty)


class TestZeroCopyConversion(object):
    """
    Tests that zero-copy conversion works with some types.
    """

    def test_zero_copy_success(self):
        result = pa.array([0, 1, 2]).to_pandas(zero_copy_only=True)
        npt.assert_array_equal(result, [0, 1, 2])

    def test_zero_copy_dictionaries(self):
        arr = pa.DictionaryArray.from_arrays(
            np.array([0, 0]),
            np.array([5]))

        result = arr.to_pandas(zero_copy_only=True)
        values = pd.Categorical([5, 5])

        tm.assert_series_equal(pd.Series(result), pd.Series(values),
                               check_names=False)

    def check_zero_copy_failure(self, arr):
        with pytest.raises(pa.ArrowInvalid):
            arr.to_pandas(zero_copy_only=True)

    def test_zero_copy_failure_on_object_types(self):
        self.check_zero_copy_failure(pa.array(['A', 'B', 'C']))

    def test_zero_copy_failure_with_int_when_nulls(self):
        self.check_zero_copy_failure(pa.array([0, 1, None]))

    def test_zero_copy_failure_with_float_when_nulls(self):
        self.check_zero_copy_failure(pa.array([0.0, 1.0, None]))

    def test_zero_copy_failure_on_bool_types(self):
        self.check_zero_copy_failure(pa.array([True, False]))

    def test_zero_copy_failure_on_list_types(self):
        arr = pa.array([[1, 2], [8, 9]], type=pa.list_(pa.int64()))
        self.check_zero_copy_failure(arr)

    def test_zero_copy_failure_on_timestamp_types(self):
        arr = np.array(['2007-07-13'], dtype='datetime64[ns]')
        self.check_zero_copy_failure(pa.array(arr))


# This function must be at the top-level for Python 2.7's multiprocessing
def _non_threaded_conversion():
    df = _alltypes_example()
    _check_pandas_roundtrip(df, use_threads=False)
    _check_pandas_roundtrip(df, use_threads=False, as_batch=True)


def _threaded_conversion():
    df = _alltypes_example()
    _check_pandas_roundtrip(df, use_threads=True)
    _check_pandas_roundtrip(df, use_threads=True, as_batch=True)


class TestConvertMisc(object):
    """
    Miscellaneous conversion tests.
    """

    type_pairs = [
        (np.int8, pa.int8()),
        (np.int16, pa.int16()),
        (np.int32, pa.int32()),
        (np.int64, pa.int64()),
        (np.uint8, pa.uint8()),
        (np.uint16, pa.uint16()),
        (np.uint32, pa.uint32()),
        (np.uint64, pa.uint64()),
        (np.float16, pa.float16()),
        (np.float32, pa.float32()),
        (np.float64, pa.float64()),
        # XXX unsupported
        # (np.dtype([('a', 'i2')]), pa.struct([pa.field('a', pa.int16())])),
        (np.object, pa.string()),
        (np.object, pa.binary()),
        (np.object, pa.binary(10)),
        (np.object, pa.list_(pa.int64())),
    ]

    def test_all_none_objects(self):
        df = pd.DataFrame({'a': [None, None, None]})
        _check_pandas_roundtrip(df)

    def test_all_none_category(self):
        df = pd.DataFrame({'a': [None, None, None]})
        df['a'] = df['a'].astype('category')
        _check_pandas_roundtrip(df)

    def test_empty_arrays(self):
        for dtype, pa_type in self.type_pairs:
            arr = np.array([], dtype=dtype)
            _check_array_roundtrip(arr, type=pa_type)

    def test_non_threaded_conversion(self):
        _non_threaded_conversion()

    def test_threaded_conversion_multiprocess(self):
        # Parallel conversion should work from child processes too (ARROW-2963)
        pool = mp.Pool(2)
        try:
            pool.apply(_threaded_conversion)
        finally:
            pool.close()
            pool.join()

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
        _check_pandas_roundtrip(df)

        arrays = [
            pd.Categorical(v1 * repeats),
            pd.Categorical(v2 * repeats),
            pd.Categorical(v3 * repeats)
        ]
        for values in arrays:
            _check_array_roundtrip(values)

    def test_empty_category(self):
        # ARROW-2443
        df = pd.DataFrame({'cat': pd.Categorical([])})
        _check_pandas_roundtrip(df)

    def test_mixed_types_fails(self):
        data = pd.DataFrame({'a': ['a', 1, 2.0]})
        with pytest.raises(pa.ArrowTypeError):
            pa.Table.from_pandas(data)

        data = pd.DataFrame({'a': [1, True]})
        with pytest.raises(pa.ArrowTypeError):
            pa.Table.from_pandas(data)

        data = pd.DataFrame({'a': ['a', 1, 2.0]})
        expected_msg = 'Conversion failed for column a'
        with pytest.raises(pa.ArrowTypeError, match=expected_msg):
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

            _check_pandas_roundtrip(df)
            _check_array_roundtrip(col)
            _check_array_roundtrip(col, mask=strided_mask)

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

        _check_pandas_roundtrip(df, schema=partial_schema,
                                expected_schema=expected_schema)

    def test_table_batch_empty_dataframe(self):
        df = pd.DataFrame({})
        _check_pandas_roundtrip(df)
        _check_pandas_roundtrip(df, as_batch=True)

        df2 = pd.DataFrame({}, index=[0, 1, 2])
        _check_pandas_roundtrip(df2, preserve_index=True)
        _check_pandas_roundtrip(df2, as_batch=True, preserve_index=True)

    def test_convert_empty_table(self):
        arr = pa.array([], type=pa.int64())
        tm.assert_almost_equal(arr.to_pandas(), np.array([], dtype=np.int64))
        arr = pa.array([], type=pa.string())
        tm.assert_almost_equal(arr.to_pandas(), np.array([], dtype=object))
        arr = pa.array([], type=pa.list_(pa.int64()))
        tm.assert_almost_equal(arr.to_pandas(), np.array([], dtype=object))
        arr = pa.array([], type=pa.struct([pa.field('a', pa.int64())]))
        tm.assert_almost_equal(arr.to_pandas(), np.array([], dtype=object))

    def test_non_natural_stride(self):
        """
        ARROW-2172: converting from a Numpy array with a stride that's
        not a multiple of itemsize.
        """
        dtype = np.dtype([('x', np.int32), ('y', np.int16)])
        data = np.array([(42, -1), (-43, 2)], dtype=dtype)
        assert data.strides == (6,)
        arr = pa.array(data['x'], type=pa.int32())
        assert arr.to_pylist() == [42, -43]
        arr = pa.array(data['y'], type=pa.int16())
        assert arr.to_pylist() == [-1, 2]

    def test_mixed_integer_columns(self):
        row = [[], []]
        df = pd.DataFrame(data=[row], columns=['foo', 123])
        expected_df = pd.DataFrame(data=[row], columns=['foo', '123'])
        _check_pandas_roundtrip(df, expected=expected_df, preserve_index=True)


def _fully_loaded_dataframe_example():
    from distutils.version import LooseVersion

    index = pd.MultiIndex.from_arrays([
        pd.date_range('2000-01-01', periods=5).repeat(2),
        np.tile(np.array(['foo', 'bar'], dtype=object), 5)
    ])

    c1 = pd.date_range('2000-01-01', periods=10)
    data = {
        0: c1,
        1: c1.tz_localize('utc'),
        2: c1.tz_localize('US/Eastern'),
        3: c1[::2].tz_localize('utc').repeat(2).astype('category'),
        4: ['foo', 'bar'] * 5,
        5: pd.Series(['foo', 'bar'] * 5).astype('category').values,
        6: [True, False] * 5,
        7: np.random.randn(10),
        8: np.random.randint(0, 100, size=10),
        9: pd.period_range('2013', periods=10, freq='M')
    }

    if LooseVersion(pd.__version__) >= '0.21':
        # There is an issue with pickling IntervalIndex in pandas 0.20.x
        data[10] = pd.interval_range(start=1, freq=1, periods=10)

    return pd.DataFrame(data, index=index)


@pytest.mark.parametrize('columns', ([b'foo'], ['foo']))
def test_roundtrip_with_bytes_unicode(columns):
    df = pd.DataFrame(columns=columns)
    table1 = pa.Table.from_pandas(df)
    table2 = pa.Table.from_pandas(table1.to_pandas())
    assert table1.equals(table2)
    assert table1.schema.equals(table2.schema)
    assert table1.schema.metadata == table2.schema.metadata


def _check_serialize_components_roundtrip(df):
    ctx = pa.default_serialization_context()

    components = ctx.serialize(df).to_components()
    deserialized = ctx.deserialize_components(components)

    tm.assert_frame_equal(df, deserialized)


def test_serialize_deserialize_pandas():
    # ARROW-1784, serialize and deserialize DataFrame by decomposing
    # BlockManager
    df = _fully_loaded_dataframe_example()
    _check_serialize_components_roundtrip(df)


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


def test_convert_unsupported_type_error_message():
    # ARROW-1454

    df = pd.DataFrame({
        't1': pd.date_range('2000-01-01', periods=20),
        't2': pd.date_range('2000-05-01', periods=20)
    })

    # timedelta64 as yet unsupported
    df['diff'] = df.t2 - df.t1

    expected_msg = 'Conversion failed for column diff with type timedelta64'
    with pytest.raises(pa.ArrowNotImplementedError, match=expected_msg):
        pa.Table.from_pandas(df)
