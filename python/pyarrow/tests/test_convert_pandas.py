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

import unittest

import numpy as np

import pandas as pd
import pandas.util.testing as tm

import pyarrow as A


class TestPandasConversion(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def _check_pandas_roundtrip(self, df, expected=None):
        table = A.from_pandas_dataframe(df)
        result = table.to_pandas()
        if expected is None:
            expected = df
        tm.assert_frame_equal(result, expected)

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

        null_mask = np.random.randint(0, 10, size=num_values) < 3
        dtypes = ['f4', 'f8']
        expected_cols = []

        arrays = []
        for name in dtypes:
            values = np.random.randn(num_values).astype(name)

            arr = A.from_pandas_series(values, null_mask)
            arrays.append(arr)

            values[null_mask] = np.nan

            expected_cols.append(values)

        ex_frame = pd.DataFrame(dict(zip(dtypes, expected_cols)),
                                columns=dtypes)

        table = A.Table.from_arrays(dtypes, arrays)
        result = table.to_pandas()
        tm.assert_frame_equal(result, ex_frame)

    def test_integer_no_nulls(self):
        data = {}

        numpy_dtypes = ['i1', 'i2', 'i4', 'i8', 'u1', 'u2', 'u4', 'u8']
        num_values = 100

        for dtype in numpy_dtypes:
            info = np.iinfo(dtype)
            values = np.random.randint(info.min,
                                       min(info.max, np.iinfo('i8').max),
                                       size=num_values)
            data[dtype] = values.astype(dtype)

        df = pd.DataFrame(data)
        self._check_pandas_roundtrip(df)

    def test_integer_with_nulls(self):
        # pandas requires upcast to float dtype

        int_dtypes = ['i1', 'i2', 'i4', 'i8', 'u1', 'u2', 'u4', 'u8']
        num_values = 100

        null_mask = np.random.randint(0, 10, size=num_values) < 3

        expected_cols = []
        arrays = []
        for name in int_dtypes:
            values = np.random.randint(0, 100, size=num_values)

            arr = A.from_pandas_series(values, null_mask)
            arrays.append(arr)

            expected = values.astype('f8')
            expected[null_mask] = np.nan

            expected_cols.append(expected)

        ex_frame = pd.DataFrame(dict(zip(int_dtypes, expected_cols)),
                                columns=int_dtypes)

        table = A.Table.from_arrays(int_dtypes, arrays)
        result = table.to_pandas()

        tm.assert_frame_equal(result, ex_frame)

    def test_boolean_no_nulls(self):
        num_values = 100

        np.random.seed(0)

        df = pd.DataFrame({'bools': np.random.randn(num_values) > 0})
        self._check_pandas_roundtrip(df)

    def test_boolean_nulls(self):
        # pandas requires upcast to object dtype
        num_values = 100
        np.random.seed(0)

        mask = np.random.randint(0, 10, size=num_values) < 3
        values = np.random.randint(0, 10, size=num_values) < 5

        arr = A.from_pandas_series(values, mask)

        expected = values.astype(object)
        expected[mask] = None

        ex_frame = pd.DataFrame({'bools': expected})

        table = A.Table.from_arrays(['bools'], [arr])
        result = table.to_pandas()

        tm.assert_frame_equal(result, ex_frame)

    def test_boolean_object_nulls(self):
        arr = np.array([False, None, True] * 100, dtype=object)
        df = pd.DataFrame({'bools': arr})
        self._check_pandas_roundtrip(df)

    def test_strings(self):
        repeats = 1000
        values = [b'foo', None, u'bar', 'qux', np.nan]
        df = pd.DataFrame({'strings': values * repeats})

        values = ['foo', None, u'bar', 'qux', None]
        expected = pd.DataFrame({'strings': values * repeats})
        self._check_pandas_roundtrip(df, expected)

    # def test_category(self):
    #     repeats = 1000
    #     values = [b'foo', None, u'bar', 'qux', np.nan]
    #     df = pd.DataFrame({'strings': values * repeats})
    #     df['strings'] = df['strings'].astype('category')
    #     self._check_pandas_roundtrip(df)
