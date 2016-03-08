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

from arrow.compat import unittest, u
import arrow


class TestScalars(unittest.TestCase):

    def test_null_singleton(self):
        with self.assertRaises(Exception):
            arrow.NAType()

    def test_bool(self):
        pass

    def test_int64(self):
        arr = arrow.from_pylist([1, 2, None])

        v = arr[0]
        assert isinstance(v, arrow.Int64Value)
        assert repr(v) == "1"
        assert v.as_py() == 1

        assert arr[2] is arrow.NA

    def test_double(self):
        arr = arrow.from_pylist([1.5, None, 3])

        v = arr[0]
        assert isinstance(v, arrow.DoubleValue)
        assert repr(v) == "1.5"
        assert v.as_py() == 1.5

        assert arr[1] is arrow.NA

        v = arr[2]
        assert v.as_py() == 3.0

    def test_string(self):
        arr = arrow.from_pylist(['foo', None, u('bar')])

        v = arr[0]
        assert isinstance(v, arrow.StringValue)
        assert repr(v) == "'foo'"
        assert v.as_py() == 'foo'

        assert arr[1] is arrow.NA

        v = arr[2].as_py()
        assert v == 'bar'
        assert isinstance(v, str)

    def test_list(self):
        arr = arrow.from_pylist([['foo', None], None, ['bar'], []])

        v = arr[0]
        assert len(v) == 2
        assert isinstance(v, arrow.ListValue)
        assert repr(v) == "['foo', None]"
        assert v.as_py() == ['foo', None]
        assert v[0].as_py() == 'foo'
        assert v[1] is arrow.NA

        assert arr[1] is arrow.NA

        v = arr[3]
        assert len(v) == 0
