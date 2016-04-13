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
import pyarrow.formatting as fmt


class TestArrayAPI(unittest.TestCase):

    def test_repr_on_pre_init_array(self):
        arr = pyarrow.array.Array()
        assert len(repr(arr)) > 0

    def test_getitem_NA(self):
        arr = pyarrow.from_pylist([1, None, 2])
        assert arr[1] is pyarrow.NA

    def test_list_format(self):
        arr = pyarrow.from_pylist([[1], None, [2, 3, None]])
        result = fmt.array_format(arr)
        expected = """\
[
  [1],
  NA,
  [2,
   3,
   NA]
]"""
        assert result == expected

    def test_string_format(self):
        arr = pyarrow.from_pylist(['', None, 'foo'])
        result = fmt.array_format(arr)
        expected = """\
[
  '',
  NA,
  'foo'
]"""
        assert result == expected

    def test_long_array_format(self):
        arr = pyarrow.from_pylist(range(100))
        result = fmt.array_format(arr, window=2)
        expected = """\
[
  0,
  1,
  ...
  98,
  99
]"""
        assert result == expected
