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

import io
import unittest

import pytest

import pyarrow as pa
from pyarrow.json import read_json, ReadOptions, ParseOptions


def test_read_options():
    cls = ReadOptions
    opts = cls()

    assert opts.block_size > 0
    opts.block_size = 12345
    assert opts.block_size == 12345

    assert opts.use_threads is True
    opts.use_threads = False
    assert opts.use_threads is False

    opts = cls(block_size=1234, use_threads=False)
    assert opts.block_size == 1234
    assert opts.use_threads is False


def test_parse_options():
    cls = ParseOptions
    opts = cls()
    assert opts.newlines_in_values is False
    assert opts.explicit_schema is None

    opts.newlines_in_values = True
    assert opts.newlines_in_values is True

    schema = pa.schema([pa.field('foo', pa.int32())])
    opts.explicit_schema = schema
    assert opts.explicit_schema == schema


class BaseTestJSONRead:

    def read_bytes(self, b, **kwargs):
        return self.read_json(pa.py_buffer(b), **kwargs)

    def check_names(self, table, names):
        assert table.num_columns == len(names)
        assert [c.name for c in table.columns] == names

    def test_file_object(self):
        data = b'{"a": 1, "b": 2}\n'
        expected_data = {'a': [1], 'b': [2]}
        bio = io.BytesIO(data)
        table = self.read_json(bio)
        assert table.to_pydict() == expected_data
        # Text files not allowed
        sio = io.StringIO(data.decode())
        with pytest.raises(TypeError):
            self.read_json(sio)

    def test_simple_ints(self):
        # Infer integer columns
        rows = b'{"a": 1,"b": 2, "c": 3}\n{"a": 4,"b": 5, "c": 6}\n'
        table = self.read_bytes(rows)
        schema = pa.schema([('a', pa.int64()),
                            ('b', pa.int64()),
                            ('c', pa.int64())])
        assert table.schema == schema
        assert table.to_pydict() == {
            'a': [1, 4],
            'b': [2, 5],
            'c': [3, 6],
            }

    def test_simple_varied(self):
        # Infer various kinds of data
        rows = (b'{"a": 1,"b": 2, "c": "3", "d": false}\n'
                b'{"a": 4.0, "b": -5, "c": "foo", "d": true}\n')
        table = self.read_bytes(rows)
        schema = pa.schema([('a', pa.float64()),
                            ('b', pa.int64()),
                            ('c', pa.string()),
                            ('d', pa.bool_())])
        assert table.schema == schema
        assert table.to_pydict() == {
            'a': [1.0, 4.0],
            'b': [2, -5],
            'c': [u"3", u"foo"],
            'd': [False, True],
            }

    def test_simple_nulls(self):
        # Infer various kinds of data, with nulls
        rows = (b'{"a": 1, "b": 2, "c": null, "d": null, "e": null}\n'
                b'{"a": null, "b": -5, "c": "foo", "d": null, "e": true}\n'
                b'{"a": 4.5, "b": null, "c": "nan", "d": null,"e": false}\n')
        table = self.read_bytes(rows)
        schema = pa.schema([('a', pa.float64()),
                            ('b', pa.int64()),
                            ('c', pa.string()),
                            ('d', pa.null()),
                            ('e', pa.bool_())])
        assert table.schema == schema
        assert table.to_pydict() == {
            'a': [1.0, None, 4.5],
            'b': [2, -5, None],
            'c': [None, u"foo", u"nan"],
            'd': [None, None, None],
            'e': [None, True, False],
            }


class TestSerialJSONRead(BaseTestJSONRead, unittest.TestCase):

    def read_json(self, *args, **kwargs):
        read_options = kwargs.setdefault('read_options', ReadOptions())
        read_options.use_threads = False
        table = read_json(*args, **kwargs)
        table.validate()
        return table


class TestParallelJSONRead(BaseTestJSONRead, unittest.TestCase):

    def read_json(self, *args, **kwargs):
        read_options = kwargs.setdefault('read_options', ReadOptions())
        read_options.use_threads = True
        table = read_json(*args, **kwargs)
        table.validate()
        return table
