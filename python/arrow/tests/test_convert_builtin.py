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

from arrow.compat import unittest
import arrow


class TestConvertList(unittest.TestCase):

    def test_boolean(self):
        pass

    def test_empty_list(self):
        arr = arrow.from_pylist([])
        assert len(arr) == 0
        assert arr.null_count == 0
        assert arr.type == arrow.null()

    def test_all_none(self):
        arr = arrow.from_pylist([None, None])
        assert len(arr) == 2
        assert arr.null_count == 2
        assert arr.type == arrow.null()

    def test_integer(self):
        arr = arrow.from_pylist([1, None, 3, None])
        assert len(arr) == 4
        assert arr.null_count == 2
        assert arr.type == arrow.int64()

    def test_garbage_collection(self):
        import gc
        bytes_before = arrow.total_allocated_bytes()
        arrow.from_pylist([1, None, 3, None])
        gc.collect()
        assert arrow.total_allocated_bytes() == bytes_before

    def test_double(self):
        data = [1.5, 1, None, 2.5, None, None]
        arr = arrow.from_pylist(data)
        assert len(arr) == 6
        assert arr.null_count == 3
        assert arr.type == arrow.double()

    def test_string(self):
        data = ['foo', b'bar', None, 'arrow']
        arr = arrow.from_pylist(data)
        assert len(arr) == 4
        assert arr.null_count == 1
        assert arr.type == arrow.string()

    def test_mixed_nesting_levels(self):
        arrow.from_pylist([1, 2, None])
        arrow.from_pylist([[1], [2], None])
        arrow.from_pylist([[1], [2], [None]])

        with self.assertRaises(arrow.ArrowException):
            arrow.from_pylist([1, 2, [1]])

        with self.assertRaises(arrow.ArrowException):
            arrow.from_pylist([1, 2, []])

        with self.assertRaises(arrow.ArrowException):
            arrow.from_pylist([[1], [2], [None, [1]]])

    def test_list_of_int(self):
        data = [[1, 2, 3], [], None, [1, 2]]
        arr = arrow.from_pylist(data)
        assert len(arr) == 4
        assert arr.null_count == 1
        assert arr.type == arrow.list_(arrow.int64())
