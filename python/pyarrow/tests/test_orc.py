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

from collections import OrderedDict
import bz2
import datetime
import gzip
import json
import os
import pickle
import sys

import pytest

import pyarrow as pa


# Marks all of the tests in this module
# Ignore these with pytest ... -m 'not orc'
pytestmark = pytest.mark.orc


here = os.path.abspath(os.path.dirname(__file__))
orc_data_dir = os.path.join(here, 'data', 'orc')


def path_for_orc_example(name):
    return os.path.join(orc_data_dir, '%s.orc' % name)


def path_for_json_example(name):
    return os.path.join(orc_data_dir, '%s.jsn.gz' % name)


def path_for_pickle_example(name):
    return os.path.join(orc_data_dir, '%s.pickle.bz2' % name)


def fix_example_values(actual_cols, expected_cols):
    """
    Fix type of expected values (as read from JSON) according to
    actual ORC datatype.
    """
    for name in expected_cols:
        expected = expected_cols[name]
        if not expected:
            continue
        actual = actual_cols[name]
        typ = actual[0].__class__
        if typ is bytes:
            # bytes fields are represented as lists of ints in JSON files
            # (Python 2: need to use bytearray, not bytes)
            expected = [bytearray(v) for v in expected]
        elif issubclass(typ, datetime.datetime):
            # timestamp fields are represented as strings in JSON files
            expected = [datetime.datetime.strptime(v, '%Y-%m-%d %H:%M:%S.%f')
                        for v in expected]
        elif issubclass(typ, datetime.date):
            # date fields are represented as strings in JSON files
            expected = [datetime.datetime.strptime(v, '%Y-%m-%d').date()
                        for v in expected]

        expected_cols[name] = expected


def check_example_values(orc_dict, json_dict, json_start=None, json_stop=None):
    # Check column names
    assert list(orc_dict) == list(json_dict)
    # Check column values
    for i, (actual, expected) in enumerate(zip(orc_dict.values(),
                                               json_dict.values())):
        expected = expected[json_start:json_stop]
        assert actual == expected, \
            "Mismatch for column '{}'".format(list(json_dict)[i])


def check_example_file(orc_path, expected_cols, need_fix=False):
    """
    Check a ORC file against the expected columns dictionary.
    """
    from pyarrow import orc

    orc_file = orc.ORCFile(orc_path)
    # Exercise ORCFile.read()
    table = orc_file.read()
    assert isinstance(table, pa.Table)
    orc_cols = table.to_pydict()
    if need_fix:
        fix_example_values(orc_cols, expected_cols)
    check_example_values(orc_cols, expected_cols)
    # Exercise ORCFile.read_stripe()
    json_pos = 0
    for i in range(orc_file.nstripes):
        batch = orc_file.read_stripe(i)
        check_example_values(batch.to_pydict(), expected_cols,
                             json_start=json_pos,
                             json_stop=json_pos + len(batch))
        json_pos += len(batch)
    assert json_pos == orc_file.nrows


def check_example_using_json(example_name):
    """
    Check a ORC file example against the equivalent JSON file, as given
    in the Apache ORC repository (the JSON file has one JSON object per
    line, corresponding to one row in the ORC file).
    """
    # Read JSON file
    json_path = path_for_json_example(example_name)
    if json_path.endswith('.gz'):
        f = gzip.open(json_path, 'rb')
    else:
        f = open(json_path, 'rb')
    with f:
        lines = list(f)
    if sys.version_info[:2] == (3, 5):
        # json on Python 3.5 rejects bytes objects
        lines = [line.decode('utf8') for line in lines]
    expected_rows = [json.loads(line, object_pairs_hook=OrderedDict)
                     for line in lines]
    expected_cols = OrderedDict()
    for row in expected_rows:
        for name, value in row.items():
            expected_cols.setdefault(name, []).append(value)
    # Check expected columns for correctness
    for name, value in expected_cols.items():
        assert len(value) == len(expected_rows)

    check_example_file(path_for_orc_example(example_name), expected_cols,
                       need_fix=True)


def check_example_using_pickle(example_name):
    """
    Check a ORC file example against the equivalent pickle file.
    See data/orc/json_to_pickle.py for generation of pickle files.

    For non-tiny files, this is significantly faster than checking against
    JSON files.
    """
    pickle_path = path_for_pickle_example(example_name)
    with open(pickle_path, 'rb') as f:
        expected_cols = pickle.loads(bz2.decompress(f.read()))

    check_example_file(path_for_orc_example(example_name), expected_cols)


@pytest.mark.xfail(strict=True, reason="ARROW-3049")
def test_orcfile_empty():
    check_example_using_json('TestOrcFile.emptyFile')


def test_orcfile_test1_json():
    # Exercise the JSON test path
    check_example_using_json('TestOrcFile.test1')


def test_orcfile_test1_pickle():
    check_example_using_pickle('TestOrcFile.test1')


def test_orcfile_dates():
    check_example_using_pickle('TestOrcFile.testDate1900')
