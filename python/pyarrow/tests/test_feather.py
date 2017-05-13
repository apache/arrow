# Copyright 2016 Feather Developers
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import unittest
import pytest

from numpy.testing import assert_array_equal
import numpy as np

from pandas.util.testing import assert_frame_equal
import pandas as pd

import pyarrow as pa
from pyarrow.compat import guid
from pyarrow.feather import (read_feather, write_feather,
                             FeatherReader)
from pyarrow.lib import FeatherWriter


def random_path():
    return 'feather_{}'.format(guid())


class TestFeatherReader(unittest.TestCase):

    def setUp(self):
        self.test_files = []

    def tearDown(self):
        for path in self.test_files:
            try:
                os.remove(path)
            except os.error:
                pass

    def test_file_not_exist(self):
        with self.assertRaises(pa.ArrowIOError):
            FeatherReader('test_invalid_file')

    def _get_null_counts(self, path, columns=None):
        reader = FeatherReader(path)
        counts = []
        for i in range(reader.num_columns):
            col = reader.get_column(i)
            if columns is None or col.name in columns:
                counts.append(col.null_count)

        return counts

    def _check_pandas_roundtrip(self, df, expected=None, path=None,
                                columns=None, null_counts=None):
        if path is None:
            path = random_path()

        self.test_files.append(path)
        write_feather(df, path)
        if not os.path.exists(path):
            raise Exception('file not written')

        result = read_feather(path, columns)
        if expected is None:
            expected = df

        assert_frame_equal(result, expected)

        if null_counts is None:
            null_counts = np.zeros(len(expected.columns))

        np.testing.assert_array_equal(self._get_null_counts(path, columns),
                                      null_counts)

    def _assert_error_on_write(self, df, exc, path=None):
        # check that we are raising the exception
        # on writing

        if path is None:
            path = random_path()

        self.test_files.append(path)

        def f():
            write_feather(df, path)

        self.assertRaises(exc, f)

    def test_num_rows_attr(self):
        df = pd.DataFrame({'foo': [1, 2, 3, 4, 5]})
        path = random_path()
        self.test_files.append(path)
        write_feather(df, path)

        reader = FeatherReader(path)
        assert reader.num_rows == len(df)

        df = pd.DataFrame({})
        path = random_path()
        self.test_files.append(path)
        write_feather(df, path)

        reader = FeatherReader(path)
        assert reader.num_rows == 0

    def test_float_no_nulls(self):
        data = {}
        numpy_dtypes = ['f4', 'f8']
        num_values = 100

        for dtype in numpy_dtypes:
            values = np.random.randn(num_values)
            data[dtype] = values.astype(dtype)

        df = pd.DataFrame(data)
        self._check_pandas_roundtrip(df)

    def test_float_nulls(self):
        num_values = 100

        path = random_path()
        self.test_files.append(path)
        writer = FeatherWriter()
        writer.open(path)

        null_mask = np.random.randint(0, 10, size=num_values) < 3
        dtypes = ['f4', 'f8']
        expected_cols = []
        null_counts = []
        for name in dtypes:
            values = np.random.randn(num_values).astype(name)
            writer.write_array(name, values, null_mask)

            values[null_mask] = np.nan

            expected_cols.append(values)
            null_counts.append(null_mask.sum())

        writer.close()

        ex_frame = pd.DataFrame(dict(zip(dtypes, expected_cols)),
                                columns=dtypes)

        result = read_feather(path)
        assert_frame_equal(result, ex_frame)
        assert_array_equal(self._get_null_counts(path), null_counts)

    def test_integer_no_nulls(self):
        data = {}

        numpy_dtypes = ['i1', 'i2', 'i4', 'i8',
                        'u1', 'u2', 'u4', 'u8']
        num_values = 100

        for dtype in numpy_dtypes:
            values = np.random.randint(0, 100, size=num_values)
            data[dtype] = values.astype(dtype)

        df = pd.DataFrame(data)
        self._check_pandas_roundtrip(df)

    def test_platform_numpy_integers(self):
        data = {}

        numpy_dtypes = ['longlong']
        num_values = 100

        for dtype in numpy_dtypes:
            values = np.random.randint(0, 100, size=num_values)
            data[dtype] = values.astype(dtype)

        df = pd.DataFrame(data)
        self._check_pandas_roundtrip(df)

    def test_integer_with_nulls(self):
        # pandas requires upcast to float dtype
        path = random_path()
        self.test_files.append(path)

        int_dtypes = ['i1', 'i2', 'i4', 'i8', 'u1', 'u2', 'u4', 'u8']
        num_values = 100

        writer = FeatherWriter()
        writer.open(path)

        null_mask = np.random.randint(0, 10, size=num_values) < 3
        expected_cols = []
        for name in int_dtypes:
            values = np.random.randint(0, 100, size=num_values)
            writer.write_array(name, values, null_mask)

            expected = values.astype('f8')
            expected[null_mask] = np.nan

            expected_cols.append(expected)

        ex_frame = pd.DataFrame(dict(zip(int_dtypes, expected_cols)),
                                columns=int_dtypes)

        writer.close()

        result = read_feather(path)
        assert_frame_equal(result, ex_frame)

    def test_boolean_no_nulls(self):
        num_values = 100

        np.random.seed(0)

        df = pd.DataFrame({'bools': np.random.randn(num_values) > 0})
        self._check_pandas_roundtrip(df)

    def test_boolean_nulls(self):
        # pandas requires upcast to object dtype
        path = random_path()
        self.test_files.append(path)

        num_values = 100
        np.random.seed(0)

        writer = FeatherWriter()
        writer.open(path)

        mask = np.random.randint(0, 10, size=num_values) < 3
        values = np.random.randint(0, 10, size=num_values) < 5
        writer.write_array('bools', values, mask)

        expected = values.astype(object)
        expected[mask] = None

        writer.close()

        ex_frame = pd.DataFrame({'bools': expected})

        result = read_feather(path)
        assert_frame_equal(result, ex_frame)

    def test_boolean_object_nulls(self):
        repeats = 100
        arr = np.array([False, None, True] * repeats, dtype=object)
        df = pd.DataFrame({'bools': arr})
        self._check_pandas_roundtrip(df, null_counts=[1 * repeats])

    def test_delete_partial_file_on_error(self):
        if sys.platform == 'win32':
            pytest.skip('Windows hangs on to file handle for some reason')

        # strings will fail
        df = pd.DataFrame(
            {
                'numbers': range(5),
                'strings': [b'foo', None, u'bar', 'qux', np.nan]},
            columns=['numbers', 'strings'])

        path = random_path()
        try:
            write_feather(df, path)
        except:
            pass

        assert not os.path.exists(path)

    def test_strings(self):
        repeats = 1000

        # we hvae mixed bytes, unicode, strings
        values = [b'foo', None, u'bar', 'qux', np.nan]
        df = pd.DataFrame({'strings': values * repeats})
        self._assert_error_on_write(df, ValueError)

        # embedded nulls are ok
        values = ['foo', None, 'bar', 'qux', None]
        df = pd.DataFrame({'strings': values * repeats})
        expected = pd.DataFrame({'strings': values * repeats})
        self._check_pandas_roundtrip(df, expected, null_counts=[2 * repeats])

        values = ['foo', None, 'bar', 'qux', np.nan]
        df = pd.DataFrame({'strings': values * repeats})
        expected = pd.DataFrame({'strings': values * repeats})
        self._check_pandas_roundtrip(df, expected, null_counts=[2 * repeats])

    def test_empty_strings(self):
        df = pd.DataFrame({'strings': [''] * 10})
        self._check_pandas_roundtrip(df)

    def test_nan_as_null(self):
        # Create a nan that is not numpy.nan
        values = np.array(['foo', np.nan, np.nan * 2, 'bar'] * 10)
        df = pd.DataFrame({'strings': values})
        self._check_pandas_roundtrip(df)

    def test_category(self):
        repeats = 1000
        values = ['foo', None, u'bar', 'qux', np.nan]
        df = pd.DataFrame({'strings': values * repeats})
        df['strings'] = df['strings'].astype('category')

        values = ['foo', None, 'bar', 'qux', None]
        expected = pd.DataFrame({'strings': pd.Categorical(values * repeats)})
        self._check_pandas_roundtrip(df, expected,
                                     null_counts=[2 * repeats])

    def test_timestamp(self):
        df = pd.DataFrame({'naive': pd.date_range('2016-03-28', periods=10)})
        df['with_tz'] = (df.naive.dt.tz_localize('utc')
                         .dt.tz_convert('America/Los_Angeles'))

        self._check_pandas_roundtrip(df)

    def test_timestamp_with_nulls(self):
        df = pd.DataFrame({'test': [pd.datetime(2016, 1, 1),
                                    None,
                                    pd.datetime(2016, 1, 3)]})
        df['with_tz'] = df.test.dt.tz_localize('utc')

        self._check_pandas_roundtrip(df, null_counts=[1, 1])

    @pytest.mark.xfail(reason="not supported ATM",
                       raises=NotImplementedError)
    def test_timedelta_with_nulls(self):
        df = pd.DataFrame({'test': [pd.Timedelta('1 day'),
                                    None,
                                    pd.Timedelta('3 day')]})

        self._check_pandas_roundtrip(df, null_counts=[1, 1])

    def test_out_of_float64_timestamp_with_nulls(self):
        df = pd.DataFrame(
            {'test': pd.DatetimeIndex([1451606400000000001,
                                       None, 14516064000030405])})
        df['with_tz'] = df.test.dt.tz_localize('utc')
        self._check_pandas_roundtrip(df, null_counts=[1, 1])

    def test_non_string_columns(self):
        df = pd.DataFrame({0: [1, 2, 3, 4],
                           1: [True, False, True, False]})

        expected = df.rename(columns=str)
        self._check_pandas_roundtrip(df, expected)

    def test_unicode_filename(self):
        # GH #209
        name = (b'Besa_Kavaj\xc3\xab.feather').decode('utf-8')
        df = pd.DataFrame({'foo': [1, 2, 3, 4]})
        self._check_pandas_roundtrip(df, path=name)

    def test_read_columns(self):
        data = {'foo': [1, 2, 3, 4],
                'boo': [5, 6, 7, 8],
                'woo': [1, 3, 5, 7]}
        columns = list(data.keys())[1:3]
        df = pd.DataFrame(data)
        expected = pd.DataFrame({c: data[c] for c in columns})
        self._check_pandas_roundtrip(df, expected, columns=columns)

    def test_overwritten_file(self):
        path = random_path()
        self.test_files.append(path)

        num_values = 100
        np.random.seed(0)

        values = np.random.randint(0, 10, size=num_values)
        write_feather(pd.DataFrame({'ints': values}), path)

        df = pd.DataFrame({'ints': values[0: num_values//2]})
        self._check_pandas_roundtrip(df, path=path)

    def test_filelike_objects(self):
        from io import BytesIO

        buf = BytesIO()

        # the copy makes it non-strided
        df = pd.DataFrame(np.arange(12).reshape(4, 3),
                          columns=['a', 'b', 'c']).copy()
        write_feather(df, buf)

        buf.seek(0)

        result = read_feather(buf)
        assert_frame_equal(result, df)

    def test_sparse_dataframe(self):
        # GH #221
        data = {'A': [0, 1, 2],
                'B': [1, 0, 1]}
        df = pd.DataFrame(data).to_sparse(fill_value=1)
        expected = df.to_dense()
        self._check_pandas_roundtrip(df, expected)

    def test_duplicate_columns(self):

        # https://github.com/wesm/feather/issues/53
        # not currently able to handle duplicate columns
        df = pd.DataFrame(np.arange(12).reshape(4, 3),
                          columns=list('aaa')).copy()
        self._assert_error_on_write(df, ValueError)

    def test_unsupported(self):
        # https://github.com/wesm/feather/issues/240
        # serializing actual python objects

        # period
        df = pd.DataFrame({'a': pd.period_range('2013', freq='M', periods=3)})
        self._assert_error_on_write(df, ValueError)

        # non-strings
        df = pd.DataFrame({'a': ['a', 1, 2.0]})
        self._assert_error_on_write(df, ValueError)
