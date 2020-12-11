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

import pytest

import pyarrow as pa
from pyarrow.tests.parquet.common import (_check_roundtrip,
                                          parametrize_legacy_dataset,
                                          parametrize_legacy_dataset_fixed)

try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None


try:
    import pandas as pd
    import pandas.testing as tm

    from pyarrow.tests.parquet.common import alltypes_sample
except ImportError:
    pd = tm = None

pytestmark = pytest.mark.parquet


@pytest.mark.pandas
@parametrize_legacy_dataset
def test_empty_table_roundtrip(use_legacy_dataset):
    df = alltypes_sample(size=10)

    # Create a non-empty table to infer the types correctly, then slice to 0
    table = pa.Table.from_pandas(df)
    table = pa.Table.from_arrays(
        [col.chunk(0)[:0] for col in table.itercolumns()],
        names=table.schema.names)

    assert table.schema.field('null').type == pa.null()
    assert table.schema.field('null_list').type == pa.list_(pa.null())
    _check_roundtrip(
        table, version='2.0', use_legacy_dataset=use_legacy_dataset)


@pytest.mark.pandas
@parametrize_legacy_dataset
def test_empty_table_no_columns(use_legacy_dataset):
    df = pd.DataFrame()
    empty = pa.Table.from_pandas(df, preserve_index=False)
    _check_roundtrip(empty, use_legacy_dataset=use_legacy_dataset)


@parametrize_legacy_dataset
def test_empty_lists_table_roundtrip(use_legacy_dataset):
    # ARROW-2744: Shouldn't crash when writing an array of empty lists
    arr = pa.array([[], []], type=pa.list_(pa.int32()))
    table = pa.Table.from_arrays([arr], ["A"])
    _check_roundtrip(table, use_legacy_dataset=use_legacy_dataset)


@parametrize_legacy_dataset_fixed
def test_empty_directory(tempdir, use_legacy_dataset):
    # ARROW-5310 - reading empty directory
    # fails with legacy implementation
    empty_dir = tempdir / 'dataset'
    empty_dir.mkdir()

    dataset = pq.ParquetDataset(
        empty_dir, use_legacy_dataset=use_legacy_dataset)
    result = dataset.read()
    assert result.num_rows == 0
    assert result.num_columns == 0


def test_empty_row_groups(tempdir):
    # ARROW-3020
    table = pa.Table.from_arrays([pa.array([], type='int32')], ['f0'])

    path = tempdir / 'empty_row_groups.parquet'

    num_groups = 3
    with pq.ParquetWriter(path, table.schema) as writer:
        for i in range(num_groups):
            writer.write_table(table)

    reader = pq.ParquetFile(path)
    assert reader.metadata.num_row_groups == num_groups

    for i in range(num_groups):
        assert reader.read_row_group(i).equals(table)


def test_writing_empty_lists():
    # ARROW-2591: [Python] Segmentation fault issue in pq.write_table
    arr1 = pa.array([[], []], pa.list_(pa.int32()))
    table = pa.Table.from_arrays([arr1], ['list(int32)'])
    _check_roundtrip(table)


@parametrize_legacy_dataset
def test_write_nested_zero_length_array_chunk_failure(use_legacy_dataset):
    # Bug report in ARROW-3792
    cols = OrderedDict(
        int32=pa.int32(),
        list_string=pa.list_(pa.string())
    )
    data = [[], [OrderedDict(int32=1, list_string=('G',)), ]]

    # This produces a table with a column like
    # <Column name='list_string' type=ListType(list<item: string>)>
    # [
    #   [],
    #   [
    #     [
    #       "G"
    #     ]
    #   ]
    # ]
    #
    # Each column is a ChunkedArray with 2 elements
    my_arrays = [pa.array(batch, type=pa.struct(cols)).flatten()
                 for batch in data]
    my_batches = [pa.RecordBatch.from_arrays(batch, schema=pa.schema(cols))
                  for batch in my_arrays]
    tbl = pa.Table.from_batches(my_batches, pa.schema(cols))
    _check_roundtrip(tbl, use_legacy_dataset=use_legacy_dataset)
