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

from pyarrow.compat import unittest
import pyarrow


class TestConvertList(unittest.TestCase):

    def test_boolean(self):
        arr = pyarrow.from_pylist([True, None, False, None])
        assert len(arr) == 4
        assert arr.null_count == 2
        assert arr.type == pyarrow.bool_()

    def test_empty_list(self):
        arr = pyarrow.from_pylist([])
        assert len(arr) == 0
        assert arr.null_count == 0
        assert arr.type == pyarrow.null()

    def test_all_none(self):
        arr = pyarrow.from_pylist([None, None])
        assert len(arr) == 2
        assert arr.null_count == 2
        assert arr.type == pyarrow.null()

    def test_integer(self):
        arr = pyarrow.from_pylist([1, None, 3, None])
        assert len(arr) == 4
        assert arr.null_count == 2
        assert arr.type == pyarrow.int64()

    def test_garbage_collection(self):
        import gc
        bytes_before = pyarrow.total_allocated_bytes()
        pyarrow.from_pylist([1, None, 3, None])
        gc.collect()
        assert pyarrow.total_allocated_bytes() == bytes_before

    def test_double(self):
        data = [1.5, 1, None, 2.5, None, None]
        arr = pyarrow.from_pylist(data)
        assert len(arr) == 6
        assert arr.null_count == 3
        assert arr.type == pyarrow.double()

    def test_string(self):
        data = ['foo', b'bar', None, 'arrow']
        arr = pyarrow.from_pylist(data)
        assert len(arr) == 4
        assert arr.null_count == 1
        assert arr.type == pyarrow.string()

    def test_mixed_nesting_levels(self):
        pyarrow.from_pylist([1, 2, None])
        pyarrow.from_pylist([[1], [2], None])
        pyarrow.from_pylist([[1], [2], [None]])

        with self.assertRaises(pyarrow.ArrowException):
            pyarrow.from_pylist([1, 2, [1]])

        with self.assertRaises(pyarrow.ArrowException):
            pyarrow.from_pylist([1, 2, []])

        with self.assertRaises(pyarrow.ArrowException):
            pyarrow.from_pylist([[1], [2], [None, [1]]])

    def test_list_of_int(self):
        data = [[1, 2, 3], [], None, [1, 2]]
        arr = pyarrow.from_pylist(data)
        assert len(arr) == 4
        assert arr.null_count == 1
        assert arr.type == pyarrow.list_(pyarrow.int64())
