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
import gzip
import json
import pickle

from pyarrow import orc
from pyarrow.tests.test_orc import (
    fix_example_values, path_for_json_example, path_for_orc_example,
    path_for_pickle_example)


def convert_example_file(orc_path, json_path, pickle_path):
    """
    Convert a JSON file (with each row a separate JSON object in the file)
    to a pickle file containing an OrderedDict of columns.
    """
    print("Processing '{}'...".format(json_path))

    if json_path.endswith('.gz'):
        f = gzip.open(json_path, 'r')
    else:
        f = open(json_path, 'r')
    with f:
        lines = list(f)
    json_rows = [json.loads(line, object_pairs_hook=OrderedDict)
                 for line in lines]
    json_cols = OrderedDict()
    for row in json_rows:
        for name, value in row.items():
            json_cols.setdefault(name, []).append(value)
    # Check JSON columns for correctness
    for name, value in json_cols.items():
        assert len(value) == len(json_rows)

    orc_file = orc.ORCFile(orc_path)
    assert orc_file.nrows == len(json_rows)
    orc_cols = orc_file.read().to_pydict()

    # Fix JSON columns
    assert list(orc_cols) == list(json_cols)
    fix_example_values(orc_cols, json_cols)

    print("Saving '{}'...".format(pickle_path))
    with open(pickle_path, 'wb') as f:
        f.write(bz2.compress(pickle.dumps(json_cols, protocol=2),
                             compresslevel=6))


if __name__ == "__main__":
    for name in ['TestOrcFile.test1', 'TestOrcFile.testDate1900', 'decimal']:
        convert_example_file(path_for_orc_example(name),
                             path_for_json_example(name),
                             path_for_pickle_example(name))
