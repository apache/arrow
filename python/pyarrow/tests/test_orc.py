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

import pytest
import decimal
import datetime

import pyarrow as pa

# Marks all of the tests in this module
# Ignore these with pytest ... -m 'not orc'
pytestmark = pytest.mark.orc


try:
    from pandas.testing import assert_frame_equal
    import pandas as pd
except ImportError:
    pass


@pytest.fixture(scope='module')
def datadir(base_datadir):
    return base_datadir / 'orc'


def fix_example_values(actual_cols, expected_cols):
    """
    Fix type of expected values (as read from JSON) according to
    actual ORC datatype.
    """
    for name in expected_cols:
        expected = expected_cols[name]
        actual = actual_cols[name]
        typ = actual[0].__class__
        if issubclass(typ, datetime.datetime):
            # timestamp fields are represented as strings in JSON files
            expected = pd.to_datetime(expected)
        elif issubclass(typ, datetime.date):
            # # date fields are represented as strings in JSON files
            expected = expected.dt.date
        elif typ is decimal.Decimal:
            converted_decimals = [None] * len(expected)
            # decimal fields are represented as reals in JSON files
            for i, (d, v) in enumerate(zip(actual, expected)):
                if not pd.isnull(v):
                    exp = d.as_tuple().exponent
                    factor = 10 ** -exp
                    converted_decimals[i] = (
                        decimal.Decimal(round(v * factor)).scaleb(exp))
            expected = pd.Series(converted_decimals)

        expected_cols[name] = expected


def check_example_values(orc_df, expected_df, start=None, stop=None):
    if start is not None or stop is not None:
        expected_df = expected_df[start:stop].reset_index(drop=True)
    assert_frame_equal(orc_df, expected_df, check_dtype=False)


def check_example_file(orc_path, expected_df, need_fix=False):
    """
    Check a ORC file against the expected columns dictionary.
    """
    from pyarrow import orc

    orc_file = orc.ORCFile(orc_path)
    # Exercise ORCFile.read()
    table = orc_file.read()
    assert isinstance(table, pa.Table)
    table.validate()

    # This workaround needed because of ARROW-3080
    orc_df = pd.DataFrame(table.to_pydict())

    assert set(expected_df.columns) == set(orc_df.columns)

    # reorder columns if necessary
    if not orc_df.columns.equals(expected_df.columns):
        expected_df = expected_df.reindex(columns=orc_df.columns)

    if need_fix:
        fix_example_values(orc_df, expected_df)

    check_example_values(orc_df, expected_df)
    # Exercise ORCFile.read_stripe()
    json_pos = 0
    for i in range(orc_file.nstripes):
        batch = orc_file.read_stripe(i)
        check_example_values(pd.DataFrame(batch.to_pydict()),
                             expected_df,
                             start=json_pos,
                             stop=json_pos + len(batch))
        json_pos += len(batch)
    assert json_pos == orc_file.nrows


@pytest.mark.pandas
@pytest.mark.parametrize('filename', [
    'TestOrcFile.test1.orc',
    'TestOrcFile.testDate1900.orc',
    'decimal.orc'
])
def test_example_using_json(filename, datadir):
    """
    Check a ORC file example against the equivalent JSON file, as given
    in the Apache ORC repository (the JSON file has one JSON object per
    line, corresponding to one row in the ORC file).
    """
    # Read JSON file
    path = datadir / filename
    table = pd.read_json(str(path.with_suffix('.jsn.gz')), lines=True)
    check_example_file(path, table, need_fix=True)


def test_orcfile_empty(datadir):
    from pyarrow import orc

    table = orc.ORCFile(datadir / 'TestOrcFile.emptyFile.orc').read()
    assert table.num_rows == 0

    expected_schema = pa.schema([
        ('boolean1', pa.bool_()),
        ('byte1', pa.int8()),
        ('short1', pa.int16()),
        ('int1', pa.int32()),
        ('long1', pa.int64()),
        ('float1', pa.float32()),
        ('double1', pa.float64()),
        ('bytes1', pa.binary()),
        ('string1', pa.string()),
        ('middle', pa.struct([
            ('list', pa.list_(pa.struct([
                ('int1', pa.int32()),
                ('string1', pa.string()),
            ]))),
        ])),
        ('list', pa.list_(pa.struct([
            ('int1', pa.int32()),
            ('string1', pa.string()),
        ]))),
        ('map', pa.list_(pa.struct([
            ('key', pa.string()),
            ('value', pa.struct([
                ('int1', pa.int32()),
                ('string1', pa.string()),
            ])),
        ]))),
    ])
    assert table.schema == expected_schema
