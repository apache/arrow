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
import gzip
import io
import itertools
import os
import shutil
import string
import tempfile
import unittest

import pytest

import numpy as np

import pyarrow as pa
from pyarrow.csv import read_csv, ReadOptions, ParseOptions, ConvertOptions


def generate_col_names():
    # 'a', 'b'... 'z', then 'aa', 'ab'...
    letters = string.ascii_lowercase
    for letter in letters:
        yield letter
    for first in letter:
        for second in letter:
            yield first + second


def make_random_csv(num_cols=2, num_rows=10, linesep=u'\r\n'):
    arr = np.random.RandomState(42).randint(0, 1000, size=(num_cols, num_rows))
    col_names = list(itertools.islice(generate_col_names(), num_cols))
    csv = io.StringIO()
    csv.write(u",".join(col_names))
    csv.write(linesep)
    for row in arr.T:
        csv.write(u",".join(map(str, row)))
        csv.write(linesep)
    csv = csv.getvalue().encode()
    columns = [pa.array(a, type=pa.int64()) for a in arr]
    expected = pa.Table.from_arrays(columns, col_names)
    return csv, expected


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
    assert opts.delimiter == ','
    assert opts.quote_char == '"'
    assert opts.double_quote is True
    assert opts.escape_char is False
    assert opts.header_rows == 1
    assert opts.newlines_in_values is False
    assert opts.ignore_empty_lines is True

    opts.delimiter = 'x'
    assert opts.delimiter == 'x'
    assert opts.quote_char == '"'

    opts.escape_char = 'z'
    assert opts.escape_char == 'z'
    assert opts.quote_char == '"'

    opts.quote_char = False
    assert opts.quote_char is False
    assert opts.escape_char == 'z'

    opts.escape_char = False
    assert opts.escape_char is False
    assert opts.quote_char is False

    opts.newlines_in_values = True
    assert opts.newlines_in_values is True

    opts.ignore_empty_lines = False
    assert opts.ignore_empty_lines is False

    opts.header_rows = 2
    assert opts.header_rows == 2

    opts = cls(delimiter=';', quote_char='%', double_quote=False,
               escape_char='\\', header_rows=2, newlines_in_values=True,
               ignore_empty_lines=False)
    assert opts.delimiter == ';'
    assert opts.quote_char == '%'
    assert opts.double_quote is False
    assert opts.escape_char == '\\'
    assert opts.header_rows == 2
    assert opts.newlines_in_values is True
    assert opts.ignore_empty_lines is False


def test_convert_options():
    cls = ConvertOptions
    opts = cls()

    assert opts.check_utf8 is True
    opts.check_utf8 = False
    assert opts.check_utf8 is False

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

    opts = cls(check_utf8=False, column_types={'a': pa.null()},
               null_values=['N', 'nn'], true_values=['T', 'tt'],
               false_values=['F', 'ff'])
    assert opts.check_utf8 is False
    assert opts.column_types == {'a': pa.null()}
    assert opts.null_values == ['N', 'nn']
    assert opts.false_values == ['F', 'ff']
    assert opts.true_values == ['T', 'tt']


class BaseTestCSVRead:

    def read_bytes(self, b, **kwargs):
        return self.read_csv(pa.py_buffer(b), **kwargs)

    def check_names(self, table, names):
        assert table.num_columns == len(names)
        assert [c.name for c in table.columns] == names

    def test_header(self):
        rows = b"abc,def,gh\n"
        table = self.read_bytes(rows)
        assert isinstance(table, pa.Table)
        self.check_names(table, ["abc", "def", "gh"])
        assert table.num_rows == 0

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
            'c': [u"3", u"foo"],
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
            'c': [u"", u"foo", u"nan"],
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
            'b': [u"Xxx", u"#N/A"],
            'c': [u"1", u""],
            'd': [2, None],
            }

        opts = ConvertOptions(null_values=[])
        rows = b"a,b\n#N/A,\n"
        table = self.read_bytes(rows, convert_options=opts)
        schema = pa.schema([('a', pa.string()),
                            ('b', pa.string())])
        assert table.schema == schema
        assert table.to_pydict() == {
            'a': [u"#N/A"],
            'b': [u""],
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
                                            'zz': 'null'})
        rows = b"a,b,c,d\n1,2,3,true\n4,-5,6,false\n"
        table = self.read_bytes(rows, convert_options=opts)
        schema = pa.schema([('a', pa.int64()),
                            ('b', pa.float32()),
                            ('c', pa.string()),
                            ('d', pa.bool_())])
        expected = {
            'a': [1, 4],
            'b': [2.0, -5.0],
            'c': ["3", "6"],
            'd': [True, False],
            }
        assert table.schema == schema
        assert table.to_pydict() == expected
        # Pass column_types as schema
        opts = ConvertOptions(
            column_types=pa.schema([('b', pa.float32()),
                                    ('c', pa.string()),
                                    ('d', pa.bool_()),
                                    ('zz', pa.bool_())]))
        table = self.read_bytes(rows, convert_options=opts)
        assert table.schema == schema
        assert table.to_pydict() == expected
        # One of the columns in column_types fails converting
        rows = b"a,b,c,d\n1,XXX,3,true\n4,-5,6,false\n"
        with pytest.raises(pa.ArrowInvalid) as exc:
            self.read_bytes(rows, convert_options=opts)
        err = str(exc.value)
        assert "In column #1: " in err
        assert "CSV conversion error to float: invalid value 'XXX'" in err

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
            'a;b': [u'de'],
            'c': [u'fg;eh'],
            }
        opts = ParseOptions(delimiter=';')
        table = self.read_bytes(rows, parse_options=opts)
        assert table.to_pydict() == {
            'a': [u'de,fg'],
            'b,c': [u'eh'],
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


class TestSerialCSVRead(BaseTestCSVRead, unittest.TestCase):

    def read_csv(self, *args, **kwargs):
        read_options = kwargs.setdefault('read_options', ReadOptions())
        read_options.use_threads = False
        table = read_csv(*args, **kwargs)
        table._validate()
        return table


class TestParallelCSVRead(BaseTestCSVRead, unittest.TestCase):

    def read_csv(self, *args, **kwargs):
        read_options = kwargs.setdefault('read_options', ReadOptions())
        read_options.use_threads = True
        table = read_csv(*args, **kwargs)
        table._validate()
        return table


class BaseTestCompressedCSVRead:

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='arrow-csv-test-')

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_random_csv(self):
        csv, expected = make_random_csv(num_cols=2, num_rows=100)
        csv_path = os.path.join(self.tmpdir, self.csv_filename)
        self.write_file(csv_path, csv)
        try:
            table = read_csv(csv_path)
        except pa.ArrowNotImplementedError as e:
            pytest.skip(str(e))
            return
        table._validate()
        assert table.schema == expected.schema
        assert table.equals(expected)
        assert table.to_pydict() == expected.to_pydict()


class TestGZipCSVRead(BaseTestCompressedCSVRead, unittest.TestCase):
    csv_filename = "compressed.csv.gz"

    def write_file(self, path, contents):
        with gzip.open(path, 'wb', 3) as f:
            f.write(contents)


class TestBZ2CSVRead(BaseTestCompressedCSVRead, unittest.TestCase):
    csv_filename = "compressed.csv.bz2"

    def write_file(self, path, contents):
        with bz2.BZ2File(path, 'w') as f:
            f.write(contents)
