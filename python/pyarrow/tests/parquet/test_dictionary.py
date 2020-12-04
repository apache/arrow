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

import numpy as np
import pytest

import pyarrow as pa
from pyarrow.tests import util
from pyarrow.tests.parquet.common import parametrize_legacy_dataset

try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None


try:
    import pandas as pd
    import pandas.testing as tm
except ImportError:
    pd = tm = None


def _simple_table_write_read(table, use_legacy_dataset):
    bio = pa.BufferOutputStream()
    pq.write_table(table, bio)
    contents = bio.getvalue()
    return pq.read_table(
        pa.BufferReader(contents), use_legacy_dataset=use_legacy_dataset
    )


@pytest.mark.pandas
@parametrize_legacy_dataset
def test_direct_read_dictionary(use_legacy_dataset):
    # ARROW-3325
    repeats = 10
    nunique = 5

    data = [
        [util.rands(10) for i in range(nunique)] * repeats,

    ]
    table = pa.table(data, names=['f0'])

    bio = pa.BufferOutputStream()
    pq.write_table(table, bio)
    contents = bio.getvalue()

    result = pq.read_table(pa.BufferReader(contents),
                           read_dictionary=['f0'],
                           use_legacy_dataset=use_legacy_dataset)

    # Compute dictionary-encoded subfield
    expected = pa.table([table[0].dictionary_encode()], names=['f0'])
    assert result.equals(expected)


@pytest.mark.pandas
@parametrize_legacy_dataset
def test_direct_read_dictionary_subfield(use_legacy_dataset):
    repeats = 10
    nunique = 5

    data = [
        [[util.rands(10)] for i in range(nunique)] * repeats,
    ]
    table = pa.table(data, names=['f0'])

    bio = pa.BufferOutputStream()
    pq.write_table(table, bio)
    contents = bio.getvalue()
    result = pq.read_table(pa.BufferReader(contents),
                           read_dictionary=['f0.list.item'],
                           use_legacy_dataset=use_legacy_dataset)

    arr = pa.array(data[0])
    values_as_dict = arr.values.dictionary_encode()

    inner_indices = values_as_dict.indices.cast('int32')
    new_values = pa.DictionaryArray.from_arrays(inner_indices,
                                                values_as_dict.dictionary)

    offsets = pa.array(range(51), type='int32')
    expected_arr = pa.ListArray.from_arrays(offsets, new_values)
    expected = pa.table([expected_arr], names=['f0'])

    assert result.equals(expected)
    assert result[0].num_chunks == 1


@parametrize_legacy_dataset
def test_dictionary_array_automatically_read(use_legacy_dataset):
    # ARROW-3246

    # Make a large dictionary, a little over 4MB of data
    dict_length = 4000
    dict_values = pa.array([('x' * 1000 + '_{}'.format(i))
                            for i in range(dict_length)])

    num_chunks = 10
    chunk_size = 100
    chunks = []
    for i in range(num_chunks):
        indices = np.random.randint(0, dict_length,
                                    size=chunk_size).astype(np.int32)
        chunks.append(pa.DictionaryArray.from_arrays(pa.array(indices),
                                                     dict_values))

    table = pa.table([pa.chunked_array(chunks)], names=['f0'])
    result = _simple_table_write_read(table, use_legacy_dataset)

    assert result.equals(table)

    # The only key in the metadata was the Arrow schema key
    assert result.schema.metadata is None
