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

from pyarrow.compat import unittest, u, unicode_type
import pyarrow as A


class TestScalars(unittest.TestCase):

    def test_null_singleton(self):
        with self.assertRaises(Exception):
            A.NAType()

    def test_bool(self):
        arr = A.from_pylist([True, None, False, None])

        v = arr[0]
        assert isinstance(v, A.BooleanValue)
        assert repr(v) == "True"
        assert v.as_py() == True

        assert arr[1] is A.NA

    def test_int64(self):
        arr = A.from_pylist([1, 2, None])

        v = arr[0]
        assert isinstance(v, A.Int64Value)
        assert repr(v) == "1"
        assert v.as_py() == 1

        assert arr[2] is A.NA

    def test_double(self):
        arr = A.from_pylist([1.5, None, 3])

        v = arr[0]
        assert isinstance(v, A.DoubleValue)
        assert repr(v) == "1.5"
        assert v.as_py() == 1.5

        assert arr[1] is A.NA

        v = arr[2]
        assert v.as_py() == 3.0

    def test_string_unicode(self):
        arr = A.from_pylist([u'foo', None, u'mañana'])

        v = arr[0]
        assert isinstance(v, A.StringValue)
        assert v.as_py() == 'foo'

        assert arr[1] is A.NA

        v = arr[2].as_py()
        assert v == u'mañana'
        assert isinstance(v, unicode_type)

    def test_bytes(self):
        arr = A.from_pylist([b'foo', None, u('bar')])

        v = arr[0]
        assert isinstance(v, A.BinaryValue)
        assert v.as_py() == b'foo'

        assert arr[1] is A.NA

        v = arr[2].as_py()
        assert v == b'bar'
        assert isinstance(v, bytes)

    def test_list(self):
        arr = A.from_pylist([['foo', None], None, ['bar'], []])

        v = arr[0]
        assert len(v) == 2
        assert isinstance(v, A.ListValue)
        assert repr(v) == "['foo', None]"
        assert v.as_py() == ['foo', None]
        assert v[0].as_py() == 'foo'
        assert v[1] is A.NA

        assert arr[1] is A.NA

        v = arr[3]
        assert len(v) == 0
