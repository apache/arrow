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

import bz2
from datetime import datetime
from decimal import Decimal
import gc
import gzip
import io
import itertools
import os
import pickle
import shutil
import string
import tempfile
import time
import unittest

import pytest

import numpy as np

import pyarrow as pa
from pyarrow.csv import (
    open_csv, read_csv, ReadOptions, ParseOptions, ConvertOptions, ISO8601)


def generate_col_names():
    # 'a', 'b'... 'z', then 'aa', 'ab'...
    letters = string.ascii_lowercase
    yield from letters
    for first in letters:
        for second in letters:
            yield first + second


def make_random_csv(num_cols=2, num_rows=10, linesep='\r\n'):
    arr = np.random.RandomState(42).randint(0, 1000, size=(num_cols, num_rows))
    col_names = list(itertools.islice(generate_col_names(), num_cols))
    csv = io.StringIO()
    csv.write(",".join(col_names))
    csv.write(linesep)
    for row in arr.T:
        csv.write(",".join(map(str, row)))
        csv.write(linesep)
    csv = csv.getvalue().encode()
    columns = [pa.array(a, type=pa.int64()) for a in arr]
    expected = pa.Table.from_arrays(columns, col_names)
    return csv, expected


def make_empty_csv(column_names):
    csv = io.StringIO()
    csv.write(",".join(column_names))
    csv.write("\n")
    return csv.getvalue().encode()


def check_options_class(cls, **attr_values):
    """
    Check setting and getting attributes of an *Options class.
    """
    opts = cls()

    for name, values in attr_values.items():
        assert getattr(opts, name) == values[0], \
            "incorrect default value for " + name
        for v in values:
            setattr(opts, name, v)
            assert getattr(opts, name) == v, "failed setting value"

    with pytest.raises(AttributeError):
        opts.zzz_non_existent = True

    # Check constructor named arguments
    non_defaults = {name: values[1] for name, values in attr_values.items()}
    opts = cls(**non_defaults)
    for name, value in non_defaults.items():
        assert getattr(opts, name) == value


def check_options_class_pickling(cls, **attr_values):
    opts = cls(**attr_values)
    new_opts = pickle.loads(pickle.dumps(opts,
                                         protocol=pickle.HIGHEST_PROTOCOL))
    for name, value in attr_values.items():
        assert getattr(new_opts, name) == value


def test_read_options():
    cls = ReadOptions
    opts = cls()

    check_options_class(cls, use_threads=[True, False],
                        skip_rows=[0, 3],
                        column_names=[[], ["ab", "cd"]],
                        autogenerate_column_names=[False, True],
                        encoding=['utf8', 'utf16'])

    assert opts.block_size > 0
    opts.block_size = 12345
    assert opts.block_size == 12345

    opts = cls(block_size=1234)
    assert opts.block_size == 1234


def test_parse_options():
    cls = ParseOptions

    check_options_class(cls, delimiter=[',', 'x'],
                        escape_char=[False, 'y'],
                        quote_char=['"', 'z', False],
                        double_quote=[True, False],
                        newlines_in_values=[False, True],
                        ignore_empty_lines=[True, False])

    # ParseOptions needs to be picklable for dataset
    check_options_class_pickling(cls, delimiter='x',
                                 escape_char='y',
                                 quote_char=False,
                                 double_quote=False,
                                 newlines_in_values=True,
                                 ignore_empty_lines=False)


def test_convert_options():
    cls = ConvertOptions
    opts = cls()

    check_options_class(
        cls, check_utf8=[True, False],
        strings_can_be_null=[False, True],
        include_columns=[[], ['def', 'abc']],
        include_missing_columns=[False, True],
        auto_dict_encode=[False, True],
        timestamp_parsers=[[], [ISO8601, '%y-%m']])

    assert opts.auto_dict_max_cardinality > 0
    opts.auto_dict_max_cardinality = 99999
    assert opts.auto_dict_max_cardinality == 99999

    assert opts.column_types == {}
    # Pass column_types as mapping
    opts.column_types = {'b': pa.int16(), 'c': pa.float32()}
    assert opts.column_types == {'b': pa.int16(), 'c': pa.float32()}
    opts.column_types = {'v': 'int16', 'w': 'null'}
    assert opts.column_types == {'v': pa.int16(), 'w': pa.null()}
    # Pass column_types as schema
    schema = pa.schema([('a', pa.int32()), ('b', pa.string())])
    opts.column_types = schema
    assert opts.column_types == {'a': pa.int32(), 'b': pa.string()}
    # Pass column_types as sequence
    opts.column_types = [('x', pa.binary())]
    assert opts.column_types == {'x': pa.binary()}

    with pytest.raises(TypeError, match='DataType expected'):
        opts.column_types = {'a': None}
    with pytest.raises(TypeError):
        opts.column_types = 0

    assert isinstance(opts.null_values, list)
    assert '' in opts.null_values
    assert 'N/A' in opts.null_values
    opts.null_values = ['xxx', 'yyy']
    assert opts.null_values == ['xxx', 'yyy']

    assert isinstance(opts.true_values, list)
    opts.true_values = ['xxx', 'yyy']
    assert opts.true_values == ['xxx', 'yyy']

    assert isinstance(opts.false_values, list)
    opts.false_values = ['xxx', 'yyy']
    assert opts.false_values == ['xxx', 'yyy']

    assert opts.timestamp_parsers == []
    opts.timestamp_parsers = [ISO8601]
    assert opts.timestamp_parsers == [ISO8601]

    opts = cls(column_types={'a': pa.null()},
               null_values=['N', 'nn'], true_values=['T', 'tt'],
               false_values=['F', 'ff'], auto_dict_max_cardinality=999,
               timestamp_parsers=[ISO8601, '%Y-%m-%d'])
    assert opts.column_types == {'a': pa.null()}
    assert opts.null_values == ['N', 'nn']
    assert opts.false_values == ['F', 'ff']
    assert opts.true_values == ['T', 'tt']
    assert opts.auto_dict_max_cardinality == 999
    assert opts.timestamp_parsers == [ISO8601, '%Y-%m-%d']


class BaseTestCSVRead:

    def read_bytes(self, b, **kwargs):
        return self.read_csv(pa.py_buffer(b), **kwargs)

    def check_names(self, table, names):
        assert table.num_columns == len(names)
        assert table.column_names == names

    def test_file_object(self):
        data = b"a,b\n1,2\n"
        expected_data = {'a': [1], 'b': [2]}
        bio = io.BytesIO(data)
        table = self.read_csv(bio)
        assert table.to_pydict() == expected_data
        # Text files not allowed
        sio = io.StringIO(data.decode())
        with pytest.raises(TypeError):
            self.read_csv(sio)

    def test_header(self):
        rows = b"abc,def,gh\n"
        table = self.read_bytes(rows)
        assert isinstance(table, pa.Table)
        self.check_names(table, ["abc", "def", "gh"])
        assert table.num_rows == 0

    def test_bom(self):
        rows = b"\xef\xbb\xbfa,b\n1,2\n"
        expected_data = {'a': [1], 'b': [2]}
        table = self.read_bytes(rows)
        assert table.to_pydict() == expected_data

    def test_one_chunk(self):
        # ARROW-7661: lack of newline at end of file should not produce
        # an additional chunk.
        rows = [b"a,b", b"1,2", b"3,4", b"56,78"]
        for line_ending in [b'\n', b'\r', b'\r\n']:
            for file_ending in [b'', line_ending]:
                data = line_ending.join(rows) + file_ending
                table = self.read_bytes(data)
                assert len(table.to_batches()) == 1
                assert table.to_pydict() == {
                    "a": [1, 3, 56],
                    "b": [2, 4, 78],
                }

    def test_header_skip_rows(self):
        rows = b"ab,cd\nef,gh\nij,kl\nmn,op\n"

        opts = ReadOptions()
        opts.skip_rows = 1
        table = self.read_bytes(rows, read_options=opts)
        self.check_names(table, ["ef", "gh"])
        assert table.to_pydict() == {
            "ef": ["ij", "mn"],
            "gh": ["kl", "op"],
        }

        opts.skip_rows = 3
        table = self.read_bytes(rows, read_options=opts)
        self.check_names(table, ["mn", "op"])
        assert table.to_pydict() == {
            "mn": [],
            "op": [],
        }

        opts.skip_rows = 4
        with pytest.raises(pa.ArrowInvalid):
            # Not enough rows
            table = self.read_bytes(rows, read_options=opts)

        # Can skip rows with a different number of columns
        rows = b"abcd\n,,,,,\nij,kl\nmn,op\n"
        opts.skip_rows = 2
        table = self.read_bytes(rows, read_options=opts)
        self.check_names(table, ["ij", "kl"])
        assert table.to_pydict() == {
            "ij": ["mn"],
            "kl": ["op"],
        }

    def test_header_column_names(self):
        rows = b"ab,cd\nef,gh\nij,kl\nmn,op\n"

        opts = ReadOptions()
        opts.column_names = ["x", "y"]
        table = self.read_bytes(rows, read_options=opts)
        self.check_names(table, ["x", "y"])
        assert table.to_pydict() == {
            "x": ["ab", "ef", "ij", "mn"],
            "y": ["cd", "gh", "kl", "op"],
        }

        opts.skip_rows = 3
        table = self.read_bytes(rows, read_options=opts)
        self.check_names(table, ["x", "y"])
        assert table.to_pydict() == {
            "x": ["mn"],
            "y": ["op"],
        }

        opts.skip_rows = 4
        table = self.read_bytes(rows, read_options=opts)
        self.check_names(table, ["x", "y"])
        assert table.to_pydict() == {
            "x": [],
            "y": [],
        }

        opts.skip_rows = 5
        with pytest.raises(pa.ArrowInvalid):
            # Not enough rows
            table = self.read_bytes(rows, read_options=opts)

        # Unexpected number of columns
        opts.skip_rows = 0
        opts.column_names = ["x", "y", "z"]
        with pytest.raises(pa.ArrowInvalid,
                           match="Expected 3 columns, got 2"):
            table = self.read_bytes(rows, read_options=opts)

        # Can skip rows with a different number of columns
        rows = b"abcd\n,,,,,\nij,kl\nmn,op\n"
        opts.skip_rows = 2
        opts.column_names = ["x", "y"]
        table = self.read_bytes(rows, read_options=opts)
        self.check_names(table, ["x", "y"])
        assert table.to_pydict() == {
            "x": ["ij", "mn"],
            "y": ["kl", "op"],
        }

    def test_header_autogenerate_column_names(self):
        rows = b"ab,cd\nef,gh\nij,kl\nmn,op\n"

        opts = ReadOptions()
        opts.autogenerate_column_names = True
        table = self.read_bytes(rows, read_options=opts)
        self.check_names(table, ["f0", "f1"])
        assert table.to_pydict() == {
            "f0": ["ab", "ef", "ij", "mn"],
            "f1": ["cd", "gh", "kl", "op"],
        }

        opts.skip_rows = 3
        table = self.read_bytes(rows, read_options=opts)
        self.check_names(table, ["f0", "f1"])
        assert table.to_pydict() == {
            "f0": ["mn"],
            "f1": ["op"],
        }

        # Not enough rows, impossible to infer number of columns
        opts.skip_rows = 4
        with pytest.raises(pa.ArrowInvalid):
            table = self.read_bytes(rows, read_options=opts)

    def test_include_columns(self):
        rows = b"ab,cd\nef,gh\nij,kl\nmn,op\n"

        convert_options = ConvertOptions()
        convert_options.include_columns = ['ab']
        table = self.read_bytes(rows, convert_options=convert_options)
        self.check_names(table, ["ab"])
        assert table.to_pydict() == {
            "ab": ["ef", "ij", "mn"],
        }

        # Order of include_columns is respected, regardless of CSV order
        convert_options.include_columns = ['cd', 'ab']
        table = self.read_bytes(rows, convert_options=convert_options)
        schema = pa.schema([('cd', pa.string()),
                            ('ab', pa.string())])
        assert table.schema == schema
        assert table.to_pydict() == {
            "cd": ["gh", "kl", "op"],
            "ab": ["ef", "ij", "mn"],
        }

        # Include a column not in the CSV file => raises by default
        convert_options.include_columns = ['xx', 'ab', 'yy']
        with pytest.raises(KeyError,
                           match="Column 'xx' in include_columns "
                                 "does not exist in CSV file"):
            self.read_bytes(rows, convert_options=convert_options)

    def test_include_missing_columns(self):
        rows = b"ab,cd\nef,gh\nij,kl\nmn,op\n"

        read_options = ReadOptions()
        convert_options = ConvertOptions()
        convert_options.include_columns = ['xx', 'ab', 'yy']
        convert_options.include_missing_columns = True
        table = self.read_bytes(rows, read_options=read_options,
                                convert_options=convert_options)
        schema = pa.schema([('xx', pa.null()),
                            ('ab', pa.string()),
                            ('yy', pa.null())])
        assert table.schema == schema
        assert table.to_pydict() == {
            "xx": [None, None, None],
            "ab": ["ef", "ij", "mn"],
            "yy": [None, None, None],
        }

        # Combining with `column_names`
        read_options.column_names = ["xx", "yy"]
        convert_options.include_columns = ["yy", "cd"]
        table = self.read_bytes(rows, read_options=read_options,
                                convert_options=convert_options)
        schema = pa.schema([('yy', pa.string()),
                            ('cd', pa.null())])
        assert table.schema == schema
        assert table.to_pydict() == {
            "yy": ["cd", "gh", "kl", "op"],
            "cd": [None, None, None, None],
        }

        # And with `column_types` as well
        convert_options.column_types = {"yy": pa.binary(),
                                        "cd": pa.int32()}
        table = self.read_bytes(rows, read_options=read_options,
                                convert_options=convert_options)
        schema = pa.schema([('yy', pa.binary()),
                            ('cd', pa.int32())])
        assert table.schema == schema
        assert table.to_pydict() == {
            "yy": [b"cd", b"gh", b"kl", b"op"],
            "cd": [None, None, None, None],
        }

    def test_simple_ints(self):
        # Infer integer columns
        rows = b"a,b,c\n1,2,3\n4,5,6\n"
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
        rows = b"a,b,c,d\n1,2,3,0\n4.0,-5,foo,True\n"
        table = self.read_bytes(rows)
        schema = pa.schema([('a', pa.float64()),
                            ('b', pa.int64()),
                            ('c', pa.string()),
                            ('d', pa.bool_())])
        assert table.schema == schema
        assert table.to_pydict() == {
            'a': [1.0, 4.0],
            'b': [2, -5],
            'c': ["3", "foo"],
            'd': [False, True],
        }

    def test_simple_nulls(self):
        # Infer various kinds of data, with nulls
        rows = (b"a,b,c,d,e,f\n"
                b"1,2,,,3,N/A\n"
                b"nan,-5,foo,,nan,TRUE\n"
                b"4.5,#N/A,nan,,\xff,false\n")
        table = self.read_bytes(rows)
        schema = pa.schema([('a', pa.float64()),
                            ('b', pa.int64()),
                            ('c', pa.string()),
                            ('d', pa.null()),
                            ('e', pa.binary()),
                            ('f', pa.bool_())])
        assert table.schema == schema
        assert table.to_pydict() == {
            'a': [1.0, None, 4.5],
            'b': [2, -5, None],
            'c': ["", "foo", "nan"],
            'd': [None, None, None],
            'e': [b"3", b"nan", b"\xff"],
            'f': [None, True, False],
        }

    def test_simple_timestamps(self):
        # Infer a timestamp column
        rows = b"a,b\n1970,1970-01-01\n1989,1989-07-14\n"
        table = self.read_bytes(rows)
        schema = pa.schema([('a', pa.int64()),
                            ('b', pa.timestamp('s'))])
        assert table.schema == schema
        assert table.to_pydict() == {
            'a': [1970, 1989],
            'b': [datetime(1970, 1, 1), datetime(1989, 7, 14)],
        }

    def test_timestamp_parsers(self):
        # Infer timestamps with custom parsers
        rows = b"a,b\n1970/01/01,1980-01-01\n1970/01/02,1980-01-02\n"
        opts = ConvertOptions()

        table = self.read_bytes(rows, convert_options=opts)
        schema = pa.schema([('a', pa.string()),
                            ('b', pa.timestamp('s'))])
        assert table.schema == schema
        assert table.to_pydict() == {
            'a': ['1970/01/01', '1970/01/02'],
            'b': [datetime(1980, 1, 1), datetime(1980, 1, 2)],
        }

        opts.timestamp_parsers = ['%Y/%m/%d']
        table = self.read_bytes(rows, convert_options=opts)
        schema = pa.schema([('a', pa.timestamp('s')),
                            ('b', pa.string())])
        assert table.schema == schema
        assert table.to_pydict() == {
            'a': [datetime(1970, 1, 1), datetime(1970, 1, 2)],
            'b': ['1980-01-01', '1980-01-02'],
        }

        opts.timestamp_parsers = ['%Y/%m/%d', ISO8601]
        table = self.read_bytes(rows, convert_options=opts)
        schema = pa.schema([('a', pa.timestamp('s')),
                            ('b', pa.timestamp('s'))])
        assert table.schema == schema
        assert table.to_pydict() == {
            'a': [datetime(1970, 1, 1), datetime(1970, 1, 2)],
            'b': [datetime(1980, 1, 1), datetime(1980, 1, 2)],
        }

    def test_auto_dict_encode(self):
        opts = ConvertOptions(auto_dict_encode=True)
        rows = "a,b\nab,1\ncdé,2\ncdé,3\nab,4".encode()
        table = self.read_bytes(rows, convert_options=opts)
        schema = pa.schema([('a', pa.dictionary(pa.int32(), pa.string())),
                            ('b', pa.int64())])
        expected = {
            'a': ["ab", "cdé", "cdé", "ab"],
            'b': [1, 2, 3, 4],
        }
        assert table.schema == schema
        assert table.to_pydict() == expected

        opts.auto_dict_max_cardinality = 2
        table = self.read_bytes(rows, convert_options=opts)
        assert table.schema == schema
        assert table.to_pydict() == expected

        # Cardinality above max => plain-encoded
        opts.auto_dict_max_cardinality = 1
        table = self.read_bytes(rows, convert_options=opts)
        assert table.schema == pa.schema([('a', pa.string()),
                                          ('b', pa.int64())])
        assert table.to_pydict() == expected

        # With invalid UTF8, not checked
        opts.auto_dict_max_cardinality = 50
        opts.check_utf8 = False
        rows = b"a,b\nab,1\ncd\xff,2\nab,3"
        table = self.read_bytes(rows, convert_options=opts,
                                validate_full=False)
        assert table.schema == schema
        dict_values = table['a'].chunk(0).dictionary
        assert len(dict_values) == 2
        assert dict_values[0].as_py() == "ab"
        assert dict_values[1].as_buffer() == b"cd\xff"

        # With invalid UTF8, checked
        opts.check_utf8 = True
        table = self.read_bytes(rows, convert_options=opts)
        schema = pa.schema([('a', pa.dictionary(pa.int32(), pa.binary())),
                            ('b', pa.int64())])
        expected = {
            'a': [b"ab", b"cd\xff", b"ab"],
            'b': [1, 2, 3],
        }
        assert table.schema == schema
        assert table.to_pydict() == expected

    def test_custom_nulls(self):
        # Infer nulls with custom values
        opts = ConvertOptions(null_values=['Xxx', 'Zzz'])
        rows = b"a,b,c,d\nZzz,Xxx,1,2\nXxx,#N/A,,Zzz\n"
        table = self.read_bytes(rows, convert_options=opts)
        schema = pa.schema([('a', pa.null()),
                            ('b', pa.string()),
                            ('c', pa.string()),
                            ('d', pa.int64())])
        assert table.schema == schema
        assert table.to_pydict() == {
            'a': [None, None],
            'b': ["Xxx", "#N/A"],
            'c': ["1", ""],
            'd': [2, None],
        }

        opts = ConvertOptions(null_values=['Xxx', 'Zzz'],
                              strings_can_be_null=True)
        table = self.read_bytes(rows, convert_options=opts)
        assert table.to_pydict() == {
            'a': [None, None],
            'b': [None, "#N/A"],
            'c': ["1", ""],
            'd': [2, None],
        }

        opts = ConvertOptions(null_values=[])
        rows = b"a,b\n#N/A,\n"
        table = self.read_bytes(rows, convert_options=opts)
        schema = pa.schema([('a', pa.string()),
                            ('b', pa.string())])
        assert table.schema == schema
        assert table.to_pydict() == {
            'a': ["#N/A"],
            'b': [""],
        }

    def test_custom_bools(self):
        # Infer booleans with custom values
        opts = ConvertOptions(true_values=['T', 'yes'],
                              false_values=['F', 'no'])
        rows = (b"a,b,c\n"
                b"True,T,t\n"
                b"False,F,f\n"
                b"True,yes,yes\n"
                b"False,no,no\n"
                b"N/A,N/A,N/A\n")
        table = self.read_bytes(rows, convert_options=opts)
        schema = pa.schema([('a', pa.string()),
                            ('b', pa.bool_()),
                            ('c', pa.string())])
        assert table.schema == schema
        assert table.to_pydict() == {
            'a': ["True", "False", "True", "False", "N/A"],
            'b': [True, False, True, False, None],
            'c': ["t", "f", "yes", "no", "N/A"],
        }

    def test_column_types(self):
        # Ask for specific column types in ConvertOptions
        opts = ConvertOptions(column_types={'b': 'float32',
                                            'c': 'string',
                                            'd': 'boolean',
                                            'e': pa.decimal128(11, 2),
                                            'zz': 'null'})
        rows = b"a,b,c,d,e\n1,2,3,true,1.0\n4,-5,6,false,0\n"
        table = self.read_bytes(rows, convert_options=opts)
        schema = pa.schema([('a', pa.int64()),
                            ('b', pa.float32()),
                            ('c', pa.string()),
                            ('d', pa.bool_()),
                            ('e', pa.decimal128(11, 2))])
        expected = {
            'a': [1, 4],
            'b': [2.0, -5.0],
            'c': ["3", "6"],
            'd': [True, False],
            'e': [Decimal("1.00"), Decimal("0.00")]
        }
        assert table.schema == schema
        assert table.to_pydict() == expected
        # Pass column_types as schema
        opts = ConvertOptions(
            column_types=pa.schema([('b', pa.float32()),
                                    ('c', pa.string()),
                                    ('d', pa.bool_()),
                                    ('e', pa.decimal128(11, 2)),
                                    ('zz', pa.bool_())]))
        table = self.read_bytes(rows, convert_options=opts)
        assert table.schema == schema
        assert table.to_pydict() == expected
        # One of the columns in column_types fails converting
        rows = b"a,b,c,d,e\n1,XXX,3,true,5\n4,-5,6,false,7\n"
        with pytest.raises(pa.ArrowInvalid) as exc:
            self.read_bytes(rows, convert_options=opts)
        err = str(exc.value)
        assert "In CSV column #1: " in err
        assert "CSV conversion error to float: invalid value 'XXX'" in err

    def test_column_types_with_column_names(self):
        # When both `column_names` and `column_types` are given, names
        # in `column_types` should refer to names in `column_names`
        rows = b"a,b\nc,d\ne,f\n"
        read_options = ReadOptions(column_names=['x', 'y'])
        convert_options = ConvertOptions(column_types={'x': pa.binary()})
        table = self.read_bytes(rows, read_options=read_options,
                                convert_options=convert_options)
        schema = pa.schema([('x', pa.binary()),
                            ('y', pa.string())])
        assert table.schema == schema
        assert table.to_pydict() == {
            'x': [b'a', b'c', b'e'],
            'y': ['b', 'd', 'f'],
        }

    def test_no_ending_newline(self):
        # No \n after last line
        rows = b"a,b,c\n1,2,3\n4,5,6"
        table = self.read_bytes(rows)
        assert table.to_pydict() == {
            'a': [1, 4],
            'b': [2, 5],
            'c': [3, 6],
        }

    def test_trivial(self):
        # A bit pointless, but at least it shouldn't crash
        rows = b",\n\n"
        table = self.read_bytes(rows)
        assert table.to_pydict() == {'': []}

    def test_empty_lines(self):
        rows = b"a,b\n\r1,2\r\n\r\n3,4\r\n"
        table = self.read_bytes(rows)
        assert table.to_pydict() == {
            'a': [1, 3],
            'b': [2, 4],
        }
        parse_options = ParseOptions(ignore_empty_lines=False)
        table = self.read_bytes(rows, parse_options=parse_options)
        assert table.to_pydict() == {
            'a': [None, 1, None, 3],
            'b': [None, 2, None, 4],
        }
        read_options = ReadOptions(skip_rows=2)
        table = self.read_bytes(rows, parse_options=parse_options,
                                read_options=read_options)
        assert table.to_pydict() == {
            '1': [None, 3],
            '2': [None, 4],
        }

    def test_invalid_csv(self):
        # Various CSV errors
        rows = b"a,b,c\n1,2\n4,5,6\n"
        with pytest.raises(pa.ArrowInvalid, match="Expected 3 columns, got 2"):
            self.read_bytes(rows)
        rows = b"a,b,c\n1,2,3\n4"
        with pytest.raises(pa.ArrowInvalid, match="Expected 3 columns, got 1"):
            self.read_bytes(rows)
        for rows in [b"", b"\n", b"\r\n", b"\r", b"\n\n"]:
            with pytest.raises(pa.ArrowInvalid, match="Empty CSV file"):
                self.read_bytes(rows)

    def test_options_delimiter(self):
        rows = b"a;b,c\nde,fg;eh\n"
        table = self.read_bytes(rows)
        assert table.to_pydict() == {
            'a;b': ['de'],
            'c': ['fg;eh'],
        }
        opts = ParseOptions(delimiter=';')
        table = self.read_bytes(rows, parse_options=opts)
        assert table.to_pydict() == {
            'a': ['de,fg'],
            'b,c': ['eh'],
        }

    def test_small_random_csv(self):
        csv, expected = make_random_csv(num_cols=2, num_rows=10)
        table = self.read_bytes(csv)
        assert table.schema == expected.schema
        assert table.equals(expected)
        assert table.to_pydict() == expected.to_pydict()

    def test_stress_block_sizes(self):
        # Test a number of small block sizes to stress block stitching
        csv_base, expected = make_random_csv(num_cols=2, num_rows=500)
        block_sizes = [11, 12, 13, 17, 37, 111]
        csvs = [csv_base, csv_base.rstrip(b'\r\n')]
        for csv in csvs:
            for block_size in block_sizes:
                read_options = ReadOptions(block_size=block_size)
                table = self.read_bytes(csv, read_options=read_options)
                assert table.schema == expected.schema
                if not table.equals(expected):
                    # Better error output
                    assert table.to_pydict() == expected.to_pydict()

    def test_stress_convert_options_blowup(self):
        # ARROW-6481: A convert_options with a very large number of columns
        # should not blow memory and CPU time.
        try:
            clock = time.thread_time
        except AttributeError:
            clock = time.time
        num_columns = 10000
        col_names = ["K{}".format(i) for i in range(num_columns)]
        csv = make_empty_csv(col_names)
        t1 = clock()
        convert_options = ConvertOptions(
            column_types={k: pa.string() for k in col_names[::2]})
        table = self.read_bytes(csv, convert_options=convert_options)
        dt = clock() - t1
        # Check that processing time didn't blow up.
        # This is a conservative check (it takes less than 300 ms
        # in debug mode on my local machine).
        assert dt <= 10.0
        # Check result
        assert table.num_columns == num_columns
        assert table.num_rows == 0
        assert table.column_names == col_names


class TestSerialCSVRead(BaseTestCSVRead, unittest.TestCase):

    def read_csv(self, *args, validate_full=True, **kwargs):
        read_options = kwargs.setdefault('read_options', ReadOptions())
        read_options.use_threads = False
        table = read_csv(*args, **kwargs)
        table.validate(full=validate_full)
        return table


class TestParallelCSVRead(BaseTestCSVRead, unittest.TestCase):

    def read_csv(self, *args, validate_full=True, **kwargs):
        read_options = kwargs.setdefault('read_options', ReadOptions())
        read_options.use_threads = True
        table = read_csv(*args, **kwargs)
        table.validate(full=validate_full)
        return table


class BaseTestStreamingCSVRead:

    def open_bytes(self, b, **kwargs):
        return self.open_csv(pa.py_buffer(b), **kwargs)

    def check_reader(self, reader, expected_schema, expected_data):
        assert reader.schema == expected_schema
        batches = list(reader)
        assert len(batches) == len(expected_data)
        for batch, expected_batch in zip(batches, expected_data):
            batch.validate(full=True)
            assert batch.schema == expected_schema
            assert batch.to_pydict() == expected_batch

    def test_file_object(self):
        data = b"a,b\n1,2\n3,4\n"
        expected_data = {'a': [1, 3], 'b': [2, 4]}
        bio = io.BytesIO(data)
        reader = self.open_csv(bio)
        expected_schema = pa.schema([('a', pa.int64()),
                                     ('b', pa.int64())])
        self.check_reader(reader, expected_schema, [expected_data])

    def test_header(self):
        rows = b"abc,def,gh\n"
        reader = self.open_bytes(rows)
        expected_schema = pa.schema([('abc', pa.null()),
                                     ('def', pa.null()),
                                     ('gh', pa.null())])
        self.check_reader(reader, expected_schema, [])

    def test_inference(self):
        # Inference is done on first block
        rows = b"a,b\n123,456\nabc,de\xff\ngh,ij\n"
        expected_schema = pa.schema([('a', pa.string()),
                                     ('b', pa.binary())])

        read_options = ReadOptions()
        read_options.block_size = len(rows)
        reader = self.open_bytes(rows, read_options=read_options)
        self.check_reader(reader, expected_schema,
                          [{'a': ['123', 'abc', 'gh'],
                            'b': [b'456', b'de\xff', b'ij']}])

        read_options.block_size = len(rows) - 1
        reader = self.open_bytes(rows, read_options=read_options)
        self.check_reader(reader, expected_schema,
                          [{'a': ['123', 'abc'],
                            'b': [b'456', b'de\xff']},
                           {'a': ['gh'],
                            'b': [b'ij']}])

    def test_inference_failure(self):
        # Inference on first block, then conversion failure on second block
        rows = b"a,b\n123,456\nabc,de\xff\ngh,ij\n"
        read_options = ReadOptions()
        read_options.block_size = len(rows) - 7
        reader = self.open_bytes(rows, read_options=read_options)
        expected_schema = pa.schema([('a', pa.int64()),
                                     ('b', pa.int64())])
        assert reader.schema == expected_schema
        assert reader.read_next_batch().to_pydict() == {
            'a': [123], 'b': [456]
        }
        # Second block
        with pytest.raises(ValueError,
                           match="CSV conversion error to int64"):
            reader.read_next_batch()
        # EOF
        with pytest.raises(StopIteration):
            reader.read_next_batch()

        # Inference on first block, then conversion failure on second block,
        # then success on third block
        rows = b"a,b\n1,2\nabc,def\n45,67\n"
        read_options.block_size = 8
        reader = self.open_bytes(rows, read_options=read_options)
        expected_schema = pa.schema([('a', pa.int64()),
                                     ('b', pa.int64())])
        assert reader.schema == expected_schema
        assert reader.read_next_batch().to_pydict() == {'a': [1], 'b': [2]}
        # Second block
        with pytest.raises(ValueError,
                           match="CSV conversion error to int64"):
            reader.read_next_batch()
        # Third block
        assert reader.read_next_batch().to_pydict() == {'a': [45], 'b': [67]}
        # EOF
        with pytest.raises(StopIteration):
            reader.read_next_batch()

    def test_invalid_csv(self):
        # CSV errors on first block
        rows = b"a,b\n1,2,3\n4,5\n6,7\n"
        read_options = ReadOptions()
        read_options.block_size = 10
        with pytest.raises(pa.ArrowInvalid,
                           match="Expected 2 columns, got 3"):
            reader = self.open_bytes(rows, read_options=read_options)

        # CSV errors on second block
        rows = b"a,b\n1,2\n3,4,5\n6,7\n"
        read_options.block_size = 8
        reader = self.open_bytes(rows, read_options=read_options)
        assert reader.read_next_batch().to_pydict() == {'a': [1], 'b': [2]}
        with pytest.raises(pa.ArrowInvalid,
                           match="Expected 2 columns, got 3"):
            reader.read_next_batch()
        # Cannot continue after a parse error
        with pytest.raises(StopIteration):
            reader.read_next_batch()

    def test_options_delimiter(self):
        rows = b"a;b,c\nde,fg;eh\n"
        reader = self.open_bytes(rows)
        expected_schema = pa.schema([('a;b', pa.string()),
                                     ('c', pa.string())])
        self.check_reader(reader, expected_schema,
                          [{'a;b': ['de'],
                            'c': ['fg;eh']}])

        opts = ParseOptions(delimiter=';')
        reader = self.open_bytes(rows, parse_options=opts)
        expected_schema = pa.schema([('a', pa.string()),
                                     ('b,c', pa.string())])
        self.check_reader(reader, expected_schema,
                          [{'a': ['de,fg'],
                            'b,c': ['eh']}])

    def test_no_ending_newline(self):
        # No \n after last line
        rows = b"a,b,c\n1,2,3\n4,5,6"
        reader = self.open_bytes(rows)
        expected_schema = pa.schema([('a', pa.int64()),
                                     ('b', pa.int64()),
                                     ('c', pa.int64())])
        self.check_reader(reader, expected_schema,
                          [{'a': [1, 4],
                            'b': [2, 5],
                            'c': [3, 6]}])

    def test_empty_file(self):
        with pytest.raises(ValueError, match="Empty CSV file"):
            self.open_bytes(b"")

    def test_column_options(self):
        # With column_names
        rows = b"1,2,3\n4,5,6"
        read_options = ReadOptions()
        read_options.column_names = ['d', 'e', 'f']
        reader = self.open_bytes(rows, read_options=read_options)
        expected_schema = pa.schema([('d', pa.int64()),
                                     ('e', pa.int64()),
                                     ('f', pa.int64())])
        self.check_reader(reader, expected_schema,
                          [{'d': [1, 4],
                            'e': [2, 5],
                            'f': [3, 6]}])

        # With include_columns
        convert_options = ConvertOptions()
        convert_options.include_columns = ['f', 'e']
        reader = self.open_bytes(rows, read_options=read_options,
                                 convert_options=convert_options)
        expected_schema = pa.schema([('f', pa.int64()),
                                     ('e', pa.int64())])
        self.check_reader(reader, expected_schema,
                          [{'e': [2, 5],
                            'f': [3, 6]}])

        # With column_types
        convert_options.column_types = {'e': pa.string()}
        reader = self.open_bytes(rows, read_options=read_options,
                                 convert_options=convert_options)
        expected_schema = pa.schema([('f', pa.int64()),
                                     ('e', pa.string())])
        self.check_reader(reader, expected_schema,
                          [{'e': ["2", "5"],
                            'f': [3, 6]}])

        # Missing columns in include_columns
        convert_options.include_columns = ['g', 'f', 'e']
        with pytest.raises(
                KeyError,
                match="Column 'g' in include_columns does not exist"):
            reader = self.open_bytes(rows, read_options=read_options,
                                     convert_options=convert_options)

        convert_options.include_missing_columns = True
        reader = self.open_bytes(rows, read_options=read_options,
                                 convert_options=convert_options)
        expected_schema = pa.schema([('g', pa.null()),
                                     ('f', pa.int64()),
                                     ('e', pa.string())])
        self.check_reader(reader, expected_schema,
                          [{'g': [None, None],
                            'e': ["2", "5"],
                            'f': [3, 6]}])

        convert_options.column_types = {'e': pa.string(), 'g': pa.float64()}
        reader = self.open_bytes(rows, read_options=read_options,
                                 convert_options=convert_options)
        expected_schema = pa.schema([('g', pa.float64()),
                                     ('f', pa.int64()),
                                     ('e', pa.string())])
        self.check_reader(reader, expected_schema,
                          [{'g': [None, None],
                            'e': ["2", "5"],
                            'f': [3, 6]}])

    def test_encoding(self):
        # latin-1 (invalid utf-8)
        rows = b"a,b\nun,\xe9l\xe9phant"
        read_options = ReadOptions()
        reader = self.open_bytes(rows, read_options=read_options)
        expected_schema = pa.schema([('a', pa.string()),
                                     ('b', pa.binary())])
        self.check_reader(reader, expected_schema,
                          [{'a': ["un"],
                            'b': [b"\xe9l\xe9phant"]}])

        read_options.encoding = 'latin1'
        reader = self.open_bytes(rows, read_options=read_options)
        expected_schema = pa.schema([('a', pa.string()),
                                     ('b', pa.string())])
        self.check_reader(reader, expected_schema,
                          [{'a': ["un"],
                            'b': ["éléphant"]}])

        # utf-16
        rows = (b'\xff\xfea\x00,\x00b\x00\n\x00u\x00n\x00,'
                b'\x00\xe9\x00l\x00\xe9\x00p\x00h\x00a\x00n\x00t\x00')
        read_options.encoding = 'utf16'
        reader = self.open_bytes(rows, read_options=read_options)
        expected_schema = pa.schema([('a', pa.string()),
                                     ('b', pa.string())])
        self.check_reader(reader, expected_schema,
                          [{'a': ["un"],
                            'b': ["éléphant"]}])

    def test_small_random_csv(self):
        csv, expected = make_random_csv(num_cols=2, num_rows=10)
        reader = self.open_bytes(csv)
        table = reader.read_all()
        assert table.schema == expected.schema
        assert table.equals(expected)
        assert table.to_pydict() == expected.to_pydict()

    def test_stress_block_sizes(self):
        # Test a number of small block sizes to stress block stitching
        csv_base, expected = make_random_csv(num_cols=2, num_rows=500)
        block_sizes = [19, 21, 23, 26, 37, 111]
        csvs = [csv_base, csv_base.rstrip(b'\r\n')]
        for csv in csvs:
            for block_size in block_sizes:
                # Need at least two lines for type inference
                assert csv[:block_size].count(b'\n') >= 2
                read_options = ReadOptions(block_size=block_size)
                reader = self.open_bytes(csv, read_options=read_options)
                table = reader.read_all()
                assert table.schema == expected.schema
                if not table.equals(expected):
                    # Better error output
                    assert table.to_pydict() == expected.to_pydict()


class TestSerialStreamingCSVRead(BaseTestStreamingCSVRead, unittest.TestCase):

    def open_csv(self, *args, **kwargs):
        read_options = kwargs.setdefault('read_options', ReadOptions())
        read_options.use_threads = False
        return open_csv(*args, **kwargs)

    def test_batch_lifetime(self):
        gc.collect()
        old_allocated = pa.total_allocated_bytes()

        # Memory occupation should not grow with CSV file size
        def check_one_batch(reader, expected):
            batch = reader.read_next_batch()
            assert batch.to_pydict() == expected

        rows = b"10,11\n12,13\n14,15\n16,17\n"
        read_options = ReadOptions()
        read_options.column_names = ['a', 'b']
        read_options.block_size = 6
        reader = self.open_bytes(rows, read_options=read_options)
        check_one_batch(reader, {'a': [10], 'b': [11]})
        allocated_after_first_batch = pa.total_allocated_bytes()
        check_one_batch(reader, {'a': [12], 'b': [13]})
        assert pa.total_allocated_bytes() == allocated_after_first_batch
        check_one_batch(reader, {'a': [14], 'b': [15]})
        assert pa.total_allocated_bytes() == allocated_after_first_batch
        check_one_batch(reader, {'a': [16], 'b': [17]})
        assert pa.total_allocated_bytes() == allocated_after_first_batch
        with pytest.raises(StopIteration):
            reader.read_next_batch()
        assert pa.total_allocated_bytes() == old_allocated
        reader = None
        assert pa.total_allocated_bytes() == old_allocated


class BaseTestCompressedCSVRead:

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='arrow-csv-test-')

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def read_csv(self, csv_path):
        try:
            return read_csv(csv_path)
        except pa.ArrowNotImplementedError as e:
            pytest.skip(str(e))

    def test_random_csv(self):
        csv, expected = make_random_csv(num_cols=2, num_rows=100)
        csv_path = os.path.join(self.tmpdir, self.csv_filename)
        self.write_file(csv_path, csv)
        table = self.read_csv(csv_path)
        table.validate(full=True)
        assert table.schema == expected.schema
        assert table.equals(expected)
        assert table.to_pydict() == expected.to_pydict()


class TestGZipCSVRead(BaseTestCompressedCSVRead, unittest.TestCase):
    csv_filename = "compressed.csv.gz"

    def write_file(self, path, contents):
        with gzip.open(path, 'wb', 3) as f:
            f.write(contents)

    def test_concatenated(self):
        # ARROW-5974
        csv_path = os.path.join(self.tmpdir, self.csv_filename)
        with gzip.open(csv_path, 'wb', 3) as f:
            f.write(b"ab,cd\nef,gh\n")
        with gzip.open(csv_path, 'ab', 3) as f:
            f.write(b"ij,kl\nmn,op\n")
        table = self.read_csv(csv_path)
        assert table.to_pydict() == {
            'ab': ['ef', 'ij', 'mn'],
            'cd': ['gh', 'kl', 'op'],
        }


class TestBZ2CSVRead(BaseTestCompressedCSVRead, unittest.TestCase):
    csv_filename = "compressed.csv.bz2"

    def write_file(self, path, contents):
        with bz2.BZ2File(path, 'w') as f:
            f.write(contents)


def test_read_csv_does_not_close_passed_file_handles():
    # ARROW-4823
    buf = io.BytesIO(b"a,b,c\n1,2,3\n4,5,6")
    read_csv(buf)
    assert not buf.closed
